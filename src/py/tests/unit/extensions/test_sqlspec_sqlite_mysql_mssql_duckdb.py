"""Unit tests for Vertebra SQLSpec SQLite, MySQL, MSSQL, and DuckDB adapter packages."""

import asyncio
import contextlib
import importlib.util
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any, cast

import msgspec
import pytest
from sqlglot import parse_one
from sqlspec import sql
from sqlspec.config import ADKConfig
from sqlspec.core import SQL, ParameterStyle, TypedParameter
from sqlspec.core.hashing import hash_optimized_expression
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    MigrationError,
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
from sqlspec.exceptions import (
    ImproperConfigurationError as SQLSpecImproperConfigurationError,
)
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore
from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.extensions.litestar.store import CompanionIndex
from sqlspec.migrations.context import MigrationContext
from sqlspec.utils.adapters import resolve_adapter_name, resolve_extension_store_class
from sqlspec.utils.schema import to_schema as sqlspec_to_schema

from vertebra.extensions.sqlspec.adapters.duckdb import (
    DUCKDB_COPY_FILENAME_FOLLOWS_QUERY,
    DuckDBAsyncConfig,
    DuckDBAsyncConnectionContext,
    DuckDBAsyncDataDictionary,
    DuckDBAsyncDriver,
    DuckDBAsyncSessionContext,
    DuckDBConnectionParams,
    DuckDBDriverFeatures,
    DuckDBEventQueueStore,
    DuckDBLitestarStore,
    DuckDBPoolParams,
    DuckDBSyncConfig,
    DuckDBSyncConnectionContext,
    DuckDBSyncDataDictionary,
    DuckDBSyncDriver,
    DuckDBSyncSessionContext,
    build_duckdb_copy_sql,
    description_has_arrow_divergent_types,
    duckdb_copy_filename_follows_query,
    map_duckdb_exception,
    order_duckdb_copy_parameters,
)
from vertebra.extensions.sqlspec.adapters.duckdb import (
    build_connection_config as build_duckdb_connection_config,
)
from vertebra.extensions.sqlspec.adapters.duckdb import (
    reconcile as duckdb_reconcile,
)
from vertebra.extensions.sqlspec.adapters.duckdb.adk import (
    DuckDBADKConfig,
    DuckDBAsyncADKArtifactStore,
    DuckDBAsyncADKMemoryStore,
    DuckDBAsyncADKStore,
    DuckDBSyncADKArtifactStore,
    DuckDBSyncADKMemoryStore,
    DuckDBSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.mssql import (
    MSSQLAsyncConfig,
    MSSQLAsyncConnectionContext,
    MSSQLAsyncDataDictionary,
    MSSQLAsyncDriver,
    MSSQLAsyncSessionContext,
    MSSQLConnectionParams,
    MSSQLDriverFeatures,
    MSSQLEventQueueStore,
    MSSQLLitestarStore,
    MSSQLPoolParams,
    MSSQLSyncConfig,
    MSSQLSyncConnectionContext,
    MSSQLSyncDataDictionary,
    MSSQLSyncDriver,
    MSSQLSyncSessionContext,
    map_mssql_exception,
    rewrite_mssql_placeholders,
)
from vertebra.extensions.sqlspec.adapters.mssql import (
    build_connection_config as build_mssql_connection_config,
)
from vertebra.extensions.sqlspec.adapters.mssql import (
    reconcile as mssql_reconcile,
)
from vertebra.extensions.sqlspec.adapters.mssql.adk import (
    MSSQLADKConfig,
    MSSQLAsyncADKArtifactStore,
    MSSQLAsyncADKMemoryStore,
    MSSQLAsyncADKStore,
    MSSQLSyncADKArtifactStore,
    MSSQLSyncADKMemoryStore,
    MSSQLSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.mssql.core import build_mssql_scope_where, quote_bracket_identifier
from vertebra.extensions.sqlspec.adapters.mysql import (
    MySQLAsyncConfig,
    MySQLAsyncConnectionContext,
    MySQLAsyncDataDictionary,
    MySQLAsyncDriver,
    MySQLAsyncSessionContext,
    MySQLConnectionParams,
    MySQLDriverFeatures,
    MySQLEventQueueStore,
    MySQLLitestarStore,
    MySQLPoolParams,
    MySQLSyncConfig,
    MySQLSyncConnectionContext,
    MySQLSyncDataDictionary,
    MySQLSyncDriver,
    MySQLSyncSessionContext,
    map_mysql_exception,
)
from vertebra.extensions.sqlspec.adapters.mysql import (
    build_statement_config as build_mysql_statement_config,
)
from vertebra.extensions.sqlspec.adapters.mysql import (
    reconcile as mysql_reconcile,
)
from vertebra.extensions.sqlspec.adapters.mysql.adk import (
    MySQLADKConfig,
    MySQLAsyncADKArtifactStore,
    MySQLAsyncADKMemoryStore,
    MySQLAsyncADKStore,
    MySQLSyncADKArtifactStore,
    MySQLSyncADKMemoryStore,
    MySQLSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.mysql.core import (
    format_mysql_vector_literal,
    mysql_json_dict,
    mysql_table_options,
)
from vertebra.extensions.sqlspec.adapters.sqlite import (
    SQLiteAsyncConfig,
    SQLiteAsyncConnectionContext,
    SQLiteAsyncDataDictionary,
    SQLiteAsyncDriver,
    SQLiteAsyncSessionContext,
    SQLiteConnectionParams,
    SQLiteDriverFeatures,
    SQLiteEventQueueStore,
    SQLiteLitestarStore,
    SQLitePoolParams,
    SQLiteSyncConfig,
    SQLiteSyncConnectionContext,
    SQLiteSyncDataDictionary,
    SQLiteSyncDriver,
    SQLiteSyncSessionContext,
    datetime_to_julian,
    julian_to_datetime,
)
from vertebra.extensions.sqlspec.adapters.sqlite import (
    reconcile as sqlite_reconcile,
)
from vertebra.extensions.sqlspec.adapters.sqlite.adk import (
    SQLiteADKConfig,
    SQLiteAsyncADKArtifactStore,
    SQLiteAsyncADKMemoryStore,
    SQLiteAsyncADKStore,
    SQLiteSyncADKArtifactStore,
    SQLiteSyncADKMemoryStore,
    SQLiteSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.sqlite.core import (
    cosine_similarity,
    extension_pragma_statements,
    map_sqlite_exception,
    normalize_sqlite_parameters,
    pack_embedding,
    unpack_embedding,
    validate_fts_options,
)
from vertebra.storage import ArtifactReference
from vertebra.typing import ImproperConfigurationError, MissingDependencyError


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


@pytest.mark.parametrize(
    ("slotted_cls",),
    [
        (SQLiteSyncConfig,),
        (SQLiteAsyncConfig,),
        (SQLiteSyncDriver,),
        (SQLiteAsyncDriver,),
        (SQLiteSyncConnectionContext,),
        (SQLiteAsyncConnectionContext,),
        (SQLiteSyncSessionContext,),
        (SQLiteAsyncSessionContext,),
    ],
)
def test_sqlite_adapter_classes_declare_slots_and_typed_surface(slotted_cls: type[Any]) -> None:
    """Verify SQLite adapter configs, drivers, and session/connection contexts declare `__slots__`."""
    assert "__slots__" in slotted_cls.__dict__
    assert SQLiteConnectionParams.__required_keys__ == frozenset()
    assert "share_pool" in SQLitePoolParams.__optional_keys__
    assert "reset_session_on_release" in SQLiteDriverFeatures.__optional_keys__


@pytest.mark.parametrize(
    ("config_factory", "expected_memory_cls", "expected_artifact_cls"),
    [
        (
            lambda: SQLiteSyncConfig(database=":memory:", share_pool=False),
            SQLiteSyncADKMemoryStore,
            SQLiteSyncADKArtifactStore,
        ),
        (
            lambda: SQLiteAsyncConfig(database=":memory:", share_pool=False),
            SQLiteAsyncADKMemoryStore,
            SQLiteAsyncADKArtifactStore,
        ),
    ],
)
def test_sqlite_resolves_adk_memory_and_artifact_store_classes(
    config_factory: Any,
    expected_memory_cls: type[Any],
    expected_artifact_cls: type[Any],
) -> None:
    """Verify `resolve_extension_store_class` resolves lazy ADK memory and artifact store classes."""
    cfg = config_factory()
    assert (
        resolve_extension_store_class(
            cfg,
            extension="adk",
            store_attr="adk_memory_store_class",
            store_suffix="ADKMemoryStore",
        )
        is expected_memory_cls
    )
    assert (
        resolve_extension_store_class(
            cfg,
            extension="adk",
            store_attr="adk_artifact_store_class",
            store_suffix="ADKArtifactStore",
        )
        is expected_artifact_cls
    )


@pytest.mark.anyio
async def test_sqlite_concurrent_sessions_yield_distinct_drivers_and_share_shared_memory_broker() -> None:
    """Verify concurrent sessions yield distinct drivers while shared `:memory:` URIs reuse the registry broker."""
    sync_cfg = SQLiteSyncConfig(
        connection_config={"database": "file::memory:?cache=shared", "uri": True},
        share_pool=True,
    )
    with sync_cfg.provide_session("primary", timeout=5) as drv1, sync_cfg.provide_session("primary") as drv2:
        assert drv1 is not drv2
        assert isinstance(drv1, SQLiteSyncDriver)
        assert isinstance(drv2, SQLiteSyncDriver)
        assert drv1.native_driver is drv2.native_driver
        drv1.execute_script("CREATE TABLE IF NOT EXISTS shared_sync (id INTEGER PRIMARY KEY);")
        drv1.execute("INSERT INTO shared_sync (id) VALUES (?)", (1,))
        assert drv2.select_one("SELECT id FROM shared_sync WHERE id = ?", (1,)) == {"id": 1}
    sync_cfg.close()

    async_cfg = SQLiteAsyncConfig(
        connection_config={"database": "file::memory:?cache=shared", "uri": True},
        share_pool=True,
    )
    async with async_cfg.provide_session() as adrv1, async_cfg.provide_session() as adrv2:
        assert adrv1 is not adrv2
        assert isinstance(adrv1, SQLiteAsyncDriver)
        assert isinstance(adrv2, SQLiteAsyncDriver)
        assert adrv1.native_driver is adrv2.native_driver
        await adrv1.execute_script("CREATE TABLE IF NOT EXISTS shared_async (id INTEGER PRIMARY KEY);")
        await adrv1.execute("INSERT INTO shared_async (id) VALUES (?)", (2,))
        assert await adrv2.select_one("SELECT id FROM shared_async WHERE id = ?", (2,)) == {"id": 2}
    await async_cfg.close()


@pytest.mark.parametrize("config_cls", [SQLiteSyncConfig, SQLiteAsyncConfig])
@pytest.mark.parametrize("tx_mode", ["IMMEDIATE", "EXCLUSIVE"])
def test_sqlite_readonly_uri_rejects_non_deferred_transaction_mode(
    tmp_path: Path,
    config_cls: type[Any],
    tx_mode: str,
) -> None:
    """Verify read-only SQLite URIs reject non-`DEFERRED` `default_transaction_mode` with `ImproperConfigurationError`."""
    ro_uri = f"file:{tmp_path / 'ro.db'}?mode=ro"
    with pytest.raises(ImproperConfigurationError, match="DEFERRED"):
        config_cls(
            connection_config={"database": ro_uri, "uri": True},
            driver_features={"default_transaction_mode": tx_mode},
            share_pool=False,
        )


@pytest.mark.anyio
async def test_sqlite_reset_session_on_release_and_connection_hooks(tmp_path: Path) -> None:
    """Verify `reset_session_on_release=True` rolls back uncommitted work, cleans temp tables, and runs hooks."""
    sync_hooks: list[Any] = []
    sync_cfg = SQLiteSyncConfig(
        database=str(tmp_path / "reset_sync.db"),
        share_pool=False,
        reset_session_on_release=True,
        driver_features={"pragmas": {"cache_size": -2000}, "on_connection_create": sync_hooks.append},
    )
    with sync_cfg.provide_session() as drv:
        drv.execute("CREATE TABLE t_reset (id INTEGER PRIMARY KEY);")
        drv.begin()
        drv.execute("INSERT INTO t_reset (id) VALUES (?)", (1,))
        assert drv.native_driver.in_transaction is True
    assert len(sync_hooks) == 1
    with sync_cfg.provide_connection() as conn:
        assert conn is sync_hooks[0]
        assert conn.in_transaction is False
        assert conn.select("SELECT id FROM t_reset") == []
    sync_cfg.close()

    async_hooks: list[Any] = []
    async_cfg = SQLiteAsyncConfig(
        database=str(tmp_path / "reset_async.db"),
        share_pool=False,
        reset_session_on_release=True,
        driver_features={"pragmas": {"cache_size": -2000}, "on_connection_create": async_hooks.append},
    )
    async with async_cfg.provide_session() as adrv:
        await adrv.execute("CREATE TABLE t_reset_async (id INTEGER PRIMARY KEY);")
        await adrv.begin()
        await adrv.execute("INSERT INTO t_reset_async (id) VALUES (?)", (1,))
        assert adrv.native_driver.in_transaction is True
    assert len(async_hooks) == 1
    async with async_cfg.provide_connection() as aconn:
        assert aconn is async_hooks[0]
        assert aconn.in_transaction is False
        assert await aconn.select("SELECT id FROM t_reset_async") == []
    await async_cfg.close()


@pytest.mark.anyio
async def test_sqlite_replicas_read_routing_and_select_to_storage(tmp_path: Path) -> None:
    """Verify SQLite `replicas=` routes read sessions to read-only replicas and streams `select_to_storage`."""
    primary_path = tmp_path / "primary.db"
    replica_path = tmp_path / "replica.db"
    for db_file, label in ((primary_path, "from-primary"), (replica_path, "from-replica")):
        seed_cfg = SQLiteSyncConfig(database=str(db_file), share_pool=False)
        with seed_cfg.provide_session() as drv:
            drv.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, label TEXT NOT NULL);")
            drv.execute("INSERT INTO notes (id, label) VALUES (?, ?)", (1, label))
            drv.commit()
        seed_cfg.close()

    sync_cfg = SQLiteSyncConfig(
        database=str(primary_path),
        share_pool=False,
        replicas=[f"file:{replica_path}?mode=ro"],
    )
    assert sync_cfg.has_replicas is True
    with sync_cfg.provide_replica_session() as rep_drv:
        assert rep_drv.select_one("SELECT label FROM notes WHERE id = ?", (1,)) == {"label": "from-replica"}
    routing = sync_cfg.provide_routing_session()
    assert routing.select("SELECT label FROM notes WHERE id = ?", (1,)) == [{"label": "from-replica"}]
    routing.execute("INSERT INTO notes (id, label) VALUES (2, 'written-primary')")
    with sync_cfg.provide_session() as pri_drv:
        assert pri_drv.select_one("SELECT label FROM notes WHERE id = ?", (2,)) == {"label": "written-primary"}
        export_path = tmp_path / "sqlite_sync_export.parquet"
        job = pri_drv.select_to_storage("SELECT id, label FROM notes", export_path, format_hint="parquet")
        assert export_path.exists()
        assert job.telemetry.get("rows_processed") == 2
    sync_cfg.close()

    async_cfg = SQLiteAsyncConfig(
        database=str(primary_path),
        share_pool=False,
        replicas=[f"file:{replica_path}?mode=ro"],
    )
    assert async_cfg.has_replicas is True
    async with async_cfg.provide_replica_session() as arep_drv:
        assert await arep_drv.select_one("SELECT label FROM notes WHERE id = ?", (1,)) == {"label": "from-replica"}
    arouting = async_cfg.provide_routing_session()
    assert await arouting.select("SELECT label FROM notes WHERE id = ?", (1,)) == [{"label": "from-replica"}]
    async with async_cfg.provide_session() as apri_drv:
        async_export = tmp_path / "sqlite_async_export.parquet"
        ajob = await apri_drv.select_to_storage("SELECT id, label FROM notes", async_export, format_hint="parquet")
        assert async_export.exists()
        assert ajob.telemetry.get("rows_processed") == 2
    await async_cfg.close()


def test_sqlite_adk_upstream_bases_config_slots_and_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify upstream ADK inheritance, removed fallback bases, aligned config keys, slots, and MissingDependencyError."""
    adk_module = import_module("vertebra.extensions.sqlspec.adapters.sqlite.adk")
    assert not hasattr(adk_module, "_FallbackADKMemoryStoreCommon")
    assert not hasattr(adk_module, "_FallbackADKArtifactStoreCommon")

    assert issubclass(SQLiteSyncADKMemoryStore, BaseSyncADKMemoryStore)
    assert issubclass(SQLiteAsyncADKMemoryStore, BaseAsyncADKMemoryStore)
    assert issubclass(SQLiteSyncADKArtifactStore, BaseSyncADKArtifactStore)
    assert issubclass(SQLiteAsyncADKArtifactStore, BaseAsyncADKArtifactStore)

    for store_cls in (
        SQLiteSyncADKStore,
        SQLiteAsyncADKStore,
        SQLiteSyncADKMemoryStore,
        SQLiteAsyncADKMemoryStore,
        SQLiteSyncADKArtifactStore,
        SQLiteAsyncADKArtifactStore,
    ):
        assert "__slots__" in store_cls.__dict__

    expected_keys = {
        "memory_table",
        "enable_memory",
        "memory_use_fts",
        "memory_max_results",
        "memory_retention_days",
        "artifact_table",
        "use_fts",
        "owner_id_column",
        "pragma_overrides",
        "fts_tokenize",
        "fts_detail",
    }
    config_keys = set(SQLiteADKConfig.__optional_keys__) | set(ADKConfig.__optional_keys__)
    assert expected_keys <= config_keys

    adk_file = Path(adk_module.__file__ or "")
    spec = importlib.util.spec_from_file_location("_test_sqlite_adk_missing_dep", adk_file)
    assert spec is not None
    assert spec.loader is not None
    with monkeypatch.context() as mp:
        for mod_name in list(sys.modules):
            if mod_name == "google.adk" or mod_name.startswith("google.adk."):
                mp.setitem(sys.modules, mod_name, None)
            elif mod_name == "sqlspec.extensions.adk" or mod_name.startswith("sqlspec.extensions.adk."):
                mp.delitem(sys.modules, mod_name, raising=False)
        mp.setitem(sys.modules, "google.adk", None)
        isolated_mod = importlib.util.module_from_spec(spec)
        with pytest.raises(MissingDependencyError) as exc_info:
            spec.loader.exec_module(isolated_mod)
        assert exc_info.value.package == "google-adk"
        assert exc_info.value.install_package == "vertebra-core[adk]"


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("db_target", [":memory:", "file"])
async def test_sqlite_adk_session_deltas_events_and_missing_tables(
    tmp_path: Path,
    is_async_mode: bool,
    db_target: str,
) -> None:
    """Verify SQLite ADK session deltas, merged state returns, event limits, and missing-table behavior."""
    database = ":memory:" if db_target == ":memory:" else str(tmp_path / f"adk_{is_async_mode}.db")
    adk_ext: dict[str, Any] = {
        "adk": {
            "pragma_overrides": {"synchronous": "NORMAL"},
            "memory_retention_days": 30,
        }
    }
    ts1 = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    ts2 = ts1 + timedelta(seconds=5)
    ev1: Any = {
        "id": "ev-1",
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "invocation_id": "inv-1",
        "timestamp": ts1,
        "event_data": {"author": "user", "seq": 1},
    }
    ev2: Any = {
        "id": "ev-2",
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "invocation_id": "inv-2",
        "timestamp": ts2,
        "event_data": {"author": "agent", "seq": 2},
    }
    bad_ev: Any = {**ev1, "user_id": "other-user"}
    missing_ev: Any = {**ev1, "session_id": "missing-sess"}

    if not is_async_mode:
        cfg = SQLiteSyncConfig(database=database, share_pool=False, extension_config=adk_ext)
        store = SQLiteSyncADKStore(cfg)
        assert not hasattr(store, "__dict__")
        assert store.get_session("app-1", "user-1", "sess-1") is None
        assert store.list_sessions("app-1", "user-1") == []
        assert store.get_events("app-1", "user-1", "sess-1") == []
        assert store.get_app_state("app-1") is None
        assert store.get_user_state("app-1", "user-1") is None
        assert store.get_metadata("schema_ver") is None

        store.create_tables()
        created = store.create_session(
            "sess-1",
            "app-1",
            "user-1",
            {"local": 1},
            app_state_delta={"app:theme": "dark"},
            user_state_delta={"user:lang": "en"},
        )
        assert created["state"] == {"local": 1, "app:theme": "dark", "user:lang": "en"}
        assert store.get_app_state("app-1") == {"app:theme": "dark"}
        assert store.get_user_state("app-1", "user-1") == {"user:lang": "en"}

        second = store.create_session("sess-2", "app-1", "user-1", {"local": 10})
        assert second["state"] == {"local": 10, "app:theme": "dark", "user:lang": "en"}

        updated = store.append_event_and_update_state(
            ev1,
            "app-1",
            "user-1",
            "sess-1",
            {"local": 2},
            app_state_delta={"app:count": 5},
            user_state_delta={"user:tier": "pro"},
        )
        assert updated["state"] == {
            "local": 2,
            "app:theme": "dark",
            "app:count": 5,
            "user:lang": "en",
            "user:tier": "pro",
        }
        store.append_event(ev2)
        assert [e["id"] for e in store.get_events("app-1", "user-1", "sess-1")] == ["ev-1", "ev-2"]
        assert [e["id"] for e in store.get_events("app-1", "user-1", "sess-1", limit=1)] == ["ev-2"]
        assert store.get_events("app-1", "user-1", "sess-1", limit=0) == []

        with pytest.raises(ValueError, match="event_record scope"):
            store.append_event_and_update_state(bad_ev, "app-1", "user-1", "sess-1", {})
        with pytest.raises(ValueError, match="not found"):
            store.append_event(missing_ev)

        renewed = store.get_session("app-1", "user-1", "sess-1", renew_for=60)
        assert renewed is not None
        assert renewed["update_time"] >= updated["update_time"]
        store.set_metadata("schema_ver", "1")
        assert store.get_metadata("schema_ver") == "1"
        cfg.close()
        return

    acfg = SQLiteAsyncConfig(database=database, share_pool=False, extension_config=adk_ext)
    astore = SQLiteAsyncADKStore(acfg)
    assert not hasattr(astore, "__dict__")
    assert await astore.get_session("app-1", "user-1", "sess-1") is None
    assert await astore.list_sessions("app-1", "user-1") == []
    assert await astore.get_events("app-1", "user-1", "sess-1") == []
    assert await astore.get_app_state("app-1") is None
    assert await astore.get_user_state("app-1", "user-1") is None
    assert await astore.get_metadata("schema_ver") is None

    await astore.create_tables()
    created_a = await astore.create_session(
        "sess-1",
        "app-1",
        "user-1",
        {"local": 1},
        app_state_delta={"app:theme": "dark"},
        user_state_delta={"user:lang": "en"},
    )
    assert created_a["state"] == {"local": 1, "app:theme": "dark", "user:lang": "en"}
    assert await astore.get_app_state("app-1") == {"app:theme": "dark"}
    assert await astore.get_user_state("app-1", "user-1") == {"user:lang": "en"}

    second_a = await astore.create_session("sess-2", "app-1", "user-1", {"local": 10})
    assert second_a["state"] == {"local": 10, "app:theme": "dark", "user:lang": "en"}

    updated_a = await astore.append_event_and_update_state(
        ev1,
        "app-1",
        "user-1",
        "sess-1",
        {"local": 2},
        app_state_delta={"app:count": 5},
        user_state_delta={"user:tier": "pro"},
    )
    assert updated_a["state"] == {
        "local": 2,
        "app:theme": "dark",
        "app:count": 5,
        "user:lang": "en",
        "user:tier": "pro",
    }
    await astore.append_event(ev2)
    assert [e["id"] for e in await astore.get_events("app-1", "user-1", "sess-1")] == ["ev-1", "ev-2"]
    assert [e["id"] for e in await astore.get_events("app-1", "user-1", "sess-1", limit=1)] == ["ev-2"]
    assert await astore.get_events("app-1", "user-1", "sess-1", limit=0) == []

    with pytest.raises(ValueError, match="event_record scope"):
        await astore.append_event_and_update_state(bad_ev, "app-1", "user-1", "sess-1", {})
    with pytest.raises(ValueError, match="not found"):
        await astore.append_event(missing_ev)

    renewed_a = await astore.get_session("app-1", "user-1", "sess-1", renew_for=60)
    assert renewed_a is not None
    assert renewed_a["update_time"] >= updated_a["update_time"]
    await astore.set_metadata("schema_ver", "1")
    assert await astore.get_metadata("schema_ver") == "1"
    await acfg.close()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("fts_key", ["memory_use_fts", "use_fts", "disabled"])
async def test_sqlite_adk_memory_search_and_owner_scoped_delete(
    tmp_path: Path,
    is_async_mode: bool,
    fts_key: str,
) -> None:
    """Verify SQLite ADK memory FTS5/LIKE search, cosine reranking, and owner-scoped delete_entries_by_session."""
    adk_cfg: dict[str, Any] = {
        "memory_table": "adk_memory_custom",
        "enable_memory": True,
        "memory_max_results": 10,
        "memory_retention_days": 14,
    }
    if fts_key != "disabled":
        adk_cfg[fts_key] = True
    else:
        adk_cfg["memory_use_fts"] = False

    now = datetime.now(UTC)
    old_ts = now - timedelta(days=30)
    entries: list[Any] = [
        {
            "id": "m1",
            "session_id": "s1",
            "app_name": "app-a",
            "user_id": "u1",
            "scope": "user",
            "event_id": "e1",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "postgres migration checklist"},
            "content_text": "postgres migration checklist",
            "metadata_json": {"tag": "db"},
            "inserted_at": now,
            "embedding": [0.1, 0.9],
        },
        {
            "id": "m2",
            "session_id": "s1",
            "app_name": "app-a",
            "user_id": "u2",
            "scope": "user",
            "event_id": "e2",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "postgres backup policy"},
            "content_text": "postgres backup policy",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": [0.9, 0.1],
        },
        {
            "id": "m3",
            "session_id": "s1",
            "app_name": "app-b",
            "user_id": "u1",
            "scope": "app",
            "event_id": "e3",
            "author": "agent",
            "timestamp": old_ts,
            "content_json": {"text": "postgres shared knowledge"},
            "content_text": "postgres shared knowledge",
            "metadata_json": None,
            "inserted_at": old_ts,
            "embedding": [1.0, 0.0],
        },
        {
            "id": "m4",
            "session_id": "s2",
            "app_name": "app-a",
            "user_id": "u1",
            "scope": "user",
            "event_id": "e4",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "postgres connection pool"},
            "content_text": "postgres connection pool",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": [0.95, 0.05],
        },
    ]

    if not is_async_mode:
        cfg = SQLiteSyncConfig(
            database=str(tmp_path / f"mem_sync_{fts_key}.db"),
            share_pool=False,
            extension_config={"adk": adk_cfg},
        )
        mem = SQLiteSyncADKMemoryStore(cfg)
        assert not hasattr(mem, "__dict__")
        assert mem.use_fts is (fts_key != "disabled")
        mem.create_tables()
        assert mem.insert_memory_entries(entries) == 4
        assert mem.insert_memory_entries([entries[0]]) == 0

        hits = mem.search_entries("postgres", "app-a", "u1", embedding=[1.0, 0.0])
        assert [h["id"] for h in hits] == ["m4", "m1"]
        assert mem.search_entries("postgres", "app-a", "u1", scope_filter="app") == []

        deleted = mem.delete_entries_by_session("s1", "app-a", "u1")
        assert deleted == 1
        assert [h["id"] for h in mem.search_entries("postgres", "app-a", "u1")] == ["m4"]
        assert [h["id"] for h in mem.search_entries("postgres", "app-a", "u2")] == ["m2"]
        assert [h["id"] for h in mem.search_entries("postgres", "app-b", "u1")] == ["m3"]
        assert mem.delete_entries_older_than(7, app_name="app-b") == 1
        cfg.close()
        return

    acfg = SQLiteAsyncConfig(
        database=str(tmp_path / f"mem_async_{fts_key}.db"),
        share_pool=False,
        extension_config={"adk": adk_cfg},
    )
    amem = SQLiteAsyncADKMemoryStore(acfg)
    assert not hasattr(amem, "__dict__")
    assert amem.use_fts is (fts_key != "disabled")
    await amem.create_tables()
    assert await amem.insert_memory_entries(entries) == 4
    assert await amem.insert_memory_entries([entries[0]]) == 0

    hits_a = await amem.search_entries("postgres", "app-a", "u1", embedding=[1.0, 0.0])
    assert [h["id"] for h in hits_a] == ["m4", "m1"]
    assert await amem.search_entries("postgres", "app-a", "u1", scope_filter="app") == []

    deleted_a = await amem.delete_entries_by_session("s1", "app-a", "u1")
    assert deleted_a == 1
    assert [h["id"] for h in await amem.search_entries("postgres", "app-a", "u1")] == ["m4"]
    assert [h["id"] for h in await amem.search_entries("postgres", "app-a", "u2")] == ["m2"]
    assert [h["id"] for h in await amem.search_entries("postgres", "app-b", "u1")] == ["m3"]
    assert await amem.delete_entries_older_than(7, app_name="app-b") == 1
    await acfg.close()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_sqlite_adk_artifact_versioning_and_retention(tmp_path: Path, is_async_mode: bool) -> None:
    """Verify SQLite ADK artifact versioning, ArtifactReference round-trip, key listing, and deletion."""
    now = datetime.now(UTC)
    old_ts = now - timedelta(days=10)
    v0_record: Any = {
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "filename": "report.json",
        "version": 0,
        "mime_type": "application/json",
        "canonical_uri": "memory://artifacts/report_v0.json",
        "custom_metadata": {"stage": "draft"},
        "created_at": old_ts,
    }
    v1_ref = ArtifactReference(
        uri="memory://artifacts/report_v1.json",
        version=1,
        content_type="application/json",
        size=256,
        updated=now,
        signed_url="https://example.invalid/signed",
    )
    user_record: Any = {
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": None,
        "filename": "profile.txt",
        "version": 0,
        "mime_type": "text/plain",
        "canonical_uri": "memory://artifacts/profile.txt",
        "custom_metadata": None,
        "created_at": now,
    }

    if not is_async_mode:
        cfg = SQLiteSyncConfig(
            database=str(tmp_path / "art_sync.db"),
            share_pool=False,
            extension_config={"adk": {"artifact_table": "adk_artifacts_custom"}},
        )
        art = SQLiteSyncADKArtifactStore(cfg)
        assert not hasattr(art, "__dict__")
        art.create_table()
        assert art.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 0
        art.insert_artifact(v0_record)
        assert art.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 1
        art.insert_artifact_reference(
            "app-1",
            "user-1",
            "report.json",
            v1_ref,
            session_id="sess-1",
            custom_metadata={"stage": "final"},
        )
        art.insert_artifact(user_record)

        latest = art.get_artifact("app-1", "user-1", "report.json", session_id="sess-1")
        assert latest is not None
        assert latest["version"] == 1
        ref = art.get_artifact_reference("app-1", "user-1", "report.json", session_id="sess-1", version=1)
        assert ref is not None
        assert ref.uri == "memory://artifacts/report_v1.json"
        assert ref.size == 256
        assert art.list_artifact_keys("app-1", "user-1") == ["profile.txt"]
        assert art.list_artifact_keys("app-1", "user-1", session_id="sess-1") == ["profile.txt", "report.json"]
        assert [v["version"] for v in art.list_artifact_versions("app-1", "user-1", "report.json", "sess-1")] == [0, 1]

        pruned = art.delete_artifacts_older_than(now - timedelta(days=1), app_name="app-1")
        assert [p["version"] for p in pruned] == [0]
        removed = art.delete_artifact("app-1", "user-1", "report.json", session_id="sess-1")
        assert [r["version"] for r in removed] == [1]
        assert art.get_artifact("app-1", "user-1", "report.json", session_id="sess-1") is None
        cfg.close()
        return

    acfg = SQLiteAsyncConfig(
        database=str(tmp_path / "art_async.db"),
        share_pool=False,
        extension_config={"adk": {"artifact_table": "adk_artifacts_custom"}},
    )
    aart = SQLiteAsyncADKArtifactStore(acfg)
    assert not hasattr(aart, "__dict__")
    await aart.create_table()
    assert await aart.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 0
    await aart.insert_artifact(v0_record)
    assert await aart.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 1
    await aart.insert_artifact_reference(
        "app-1",
        "user-1",
        "report.json",
        v1_ref,
        session_id="sess-1",
        custom_metadata={"stage": "final"},
    )
    await aart.insert_artifact(user_record)

    latest_a = await aart.get_artifact("app-1", "user-1", "report.json", session_id="sess-1")
    assert latest_a is not None
    assert latest_a["version"] == 1
    ref_a = await aart.get_artifact_reference("app-1", "user-1", "report.json", session_id="sess-1", version=1)
    assert ref_a is not None
    assert ref_a.uri == "memory://artifacts/report_v1.json"
    assert ref_a.size == 256
    assert await aart.list_artifact_keys("app-1", "user-1") == ["profile.txt"]
    assert await aart.list_artifact_keys("app-1", "user-1", session_id="sess-1") == ["profile.txt", "report.json"]
    assert [v["version"] for v in await aart.list_artifact_versions("app-1", "user-1", "report.json", "sess-1")] == [
        0,
        1,
    ]

    pruned_a = await aart.delete_artifacts_older_than(now - timedelta(days=1), app_name="app-1")
    assert [p["version"] for p in pruned_a] == [0]
    removed_a = await aart.delete_artifact("app-1", "user-1", "report.json", session_id="sess-1")
    assert [r["version"] for r in removed_a] == [1]
    assert await aart.get_artifact("app-1", "user-1", "report.json", session_id="sess-1") is None
    await acfg.close()


@pytest.mark.parametrize(
    "store_cls",
    [SQLiteEventQueueStore, SQLiteLitestarStore],
)
def test_sqlite_events_litestar_slots_and_reconcile_hooks(store_cls: type[Any]) -> None:
    """Verify SQLite event/Litestar stores declare `__slots__`, hooks, and lazy ADK import isolation."""
    assert "__slots__" in store_cls.__dict__
    cfg = SQLiteSyncConfig(database=":memory:")
    store = store_cls(cfg)
    assert not hasattr(store, "__dict__")
    targets = store.get_schema_targets()
    assert len(targets) == 1
    drops = store.downgrade_drops()
    assert any("DROP TABLE IF EXISTS" in d.statement for d in drops)

    if store_cls is SQLiteLitestarStore:
        companions = store.get_companion_indexes()
        assert len(companions) == 1
        assert isinstance(companions[0], CompanionIndex)
        assert "WHERE expires_at IS NOT NULL" in companions[0].ddl

    assert sqlite_reconcile.down is sqlite_reconcile.downgrade
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import vertebra.extensions.sqlspec.adapters.sqlite.reconcile; "
                "assert 'google.adk' not in sys.modules; "
                "assert 'vertebra.extensions.sqlspec.adapters.sqlite.adk' not in sys.modules"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True])
async def test_sqlite_reconcile_and_downgrade_live_stores(tmp_path: Path, is_async_mode: bool) -> None:
    """Run live SQLite reconcile -> PRAGMAs/companion indexes -> Litestar & Events CRUD -> downgrade."""
    db_path = tmp_path / ("reconcile_async.db" if is_async_mode else "reconcile_sync.db")
    ext_cfg: dict[str, Any] = {
        "litestar": {"session_table": "vtb_sessions", "pragma_profile": True},
        "events": {"queue_table": "vtb_events", "pragma_profile": True},
    }
    if is_async_mode:
        cfg = SQLiteAsyncConfig(database=str(db_path), extension_config=ext_cfg)
        try:
            async with cfg.provide_session() as driver:
                ctx = MigrationContext(config=cfg, driver=driver)
                awaitable = sqlite_reconcile.reconcile(ctx)
                assert awaitable is not None
                await awaitable
                await driver.commit()

                fk_row = await driver.select_one("PRAGMA foreign_keys")
                assert next(iter(fk_row.values())) == 1
                tables = {row["table_name"] for row in await driver.data_dictionary.get_tables(driver)}
                assert {"vtb_sessions", "vtb_events"} <= tables

                await driver.execute_script("DROP INDEX idx_vtb_sessions_expires_at;")
                await driver.execute_script("DROP INDEX idx_vtb_events_channel_status;")
                re_awaitable = sqlite_reconcile.reconcile(ctx)
                assert re_awaitable is not None
                await re_awaitable
                await driver.commit()

                lit_idx = await driver.data_dictionary.get_indexes(driver, table="vtb_sessions")
                ev_idx = await driver.data_dictionary.get_indexes(driver, table="vtb_events")
                assert any(
                    row["index_name"] == "idx_vtb_sessions_expires_at" and row.get("is_partial") for row in lit_idx
                )
                assert any(row["index_name"] == "idx_vtb_events_channel_status" for row in ev_idx)

                await driver.execute(
                    "INSERT INTO vtb_events (event_id, channel, payload_json, status) VALUES (?, ?, ?, ?)",
                    ("ev-1", "ch-a", '{"ok":true}', "pending"),
                )
                await driver.commit()
                ev_row = await driver.select_one("SELECT channel, status FROM vtb_events WHERE event_id = ?", ("ev-1",))
                assert ev_row == {"channel": "ch-a", "status": "pending"}

            lit_store = SQLiteLitestarStore(cfg)
            await lit_store.set("sess-a", b"hello", expires_in=timedelta(seconds=60))
            assert await lit_store.get("sess-a") == b"hello"
            await lit_store.delete("sess-a")
            assert await lit_store.get("sess-a") is None

            async with cfg.provide_session() as driver:
                down_awaitable = sqlite_reconcile.downgrade(MigrationContext(config=cfg, driver=driver))
                assert down_awaitable is not None
                await down_awaitable
                await driver.commit()
                tables_after = {row["table_name"] for row in await driver.data_dictionary.get_tables(driver)}
                assert "vtb_sessions" not in tables_after
                assert "vtb_events" not in tables_after
        finally:
            await cfg.close()
        return

    sync_cfg = SQLiteSyncConfig(database=str(db_path), extension_config=ext_cfg)
    try:
        with sync_cfg.provide_session() as driver:
            ctx = MigrationContext(config=sync_cfg, driver=driver)
            assert sqlite_reconcile.reconcile(ctx) is None
            driver.commit()

            fk_row = driver.select_one("PRAGMA foreign_keys")
            assert next(iter(fk_row.values())) == 1
            tables = {row["table_name"] for row in driver.data_dictionary.get_tables(driver)}
            assert {"vtb_sessions", "vtb_events"} <= tables

            driver.execute_script("DROP INDEX idx_vtb_sessions_expires_at;")
            driver.execute_script("DROP INDEX idx_vtb_events_channel_status;")
            assert sqlite_reconcile.reconcile(ctx) is None
            driver.commit()

            lit_idx = driver.data_dictionary.get_indexes(driver, table="vtb_sessions")
            ev_idx = driver.data_dictionary.get_indexes(driver, table="vtb_events")
            assert any(row["index_name"] == "idx_vtb_sessions_expires_at" and row.get("is_partial") for row in lit_idx)
            assert any(row["index_name"] == "idx_vtb_events_channel_status" for row in ev_idx)

            driver.execute(
                "INSERT INTO vtb_events (event_id, channel, payload_json, status) VALUES (?, ?, ?, ?)",
                ("ev-1", "ch-a", '{"ok":true}', "pending"),
            )
            driver.commit()
            ev_row = driver.select_one("SELECT channel, status FROM vtb_events WHERE event_id = ?", ("ev-1",))
            assert ev_row == {"channel": "ch-a", "status": "pending"}

        lit_store = SQLiteLitestarStore(sync_cfg)
        await lit_store.set("sess-s", b"sync-val", expires_in=timedelta(seconds=60))
        assert await lit_store.get("sess-s") == b"sync-val"

        with sync_cfg.provide_session() as driver:
            assert sqlite_reconcile.downgrade(MigrationContext(config=sync_cfg, driver=driver)) is None
            driver.commit()
            tables_after = {row["table_name"] for row in driver.data_dictionary.get_tables(driver)}
            assert "vtb_sessions" not in tables_after
            assert "vtb_events" not in tables_after
    finally:
        sync_cfg.close()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True])
async def test_sqlite_add_extension_migrations_runner_integration(tmp_path: Path, is_async_mode: bool) -> None:
    """Verify `add_extension_migrations` discovers and runs SQLite reconcile/downgrade via MigrationCommands."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    if is_async_mode:
        cfg = SQLiteAsyncConfig(
            database=":memory:",
            migration_config={"script_location": str(migrations_dir)},
        )
        cfg.add_extension_migrations(
            "litestar",
            "vertebra.extensions.sqlspec:adapters/sqlite",
            settings={"session_table": "runner_sessions"},
            migration_mode="reconcile",
        )
        cfg.add_extension_migrations(
            "events",
            "vertebra.extensions.sqlspec:adapters/sqlite",
            settings={"queue_table": "runner_events"},
            migration_mode="reconcile",
        )
        cfg.add_extension_migrations(
            "adk",
            "vertebra.extensions.sqlspec:adapters/sqlite",
            migration_mode="reconcile",
        )
        try:
            commands = cfg.get_migration_commands()
            assert [desc.name for desc in commands.extension_reconcilers] == ["litestar", "events", "adk"]
            upgrade_res = commands.upgrade()
            if upgrade_res is not None:
                await upgrade_res
            async with cfg.provide_session() as driver:
                await driver.execute("COMMIT")
                tables = {row["table_name"] for row in await driver.data_dictionary.get_tables(driver)}
                assert {"runner_sessions", "runner_events", "adk_session", "adk_memory"} <= tables
            downgrade_res = commands.downgrade("base")
            if downgrade_res is not None:
                await downgrade_res
            async with cfg.provide_session() as driver:
                await driver.execute("COMMIT")
                tables_after = {row["table_name"] for row in await driver.data_dictionary.get_tables(driver)}
                assert not ({"runner_sessions", "runner_events", "adk_session", "adk_memory"} & tables_after)
        finally:
            await cfg.close()
        return

    sync_cfg = SQLiteSyncConfig(
        database=":memory:",
        migration_config={"script_location": str(migrations_dir)},
    )
    sync_cfg.add_extension_migrations(
        "litestar",
        "vertebra.extensions.sqlspec:adapters/sqlite",
        settings={"session_table": "runner_sessions"},
        migration_mode="reconcile",
    )
    sync_cfg.add_extension_migrations(
        "events",
        "vertebra.extensions.sqlspec:adapters/sqlite",
        settings={"queue_table": "runner_events"},
        migration_mode="reconcile",
    )
    sync_cfg.add_extension_migrations(
        "adk",
        "vertebra.extensions.sqlspec:adapters/sqlite",
        migration_mode="reconcile",
    )
    try:
        commands = sync_cfg.get_migration_commands()
        assert [desc.name for desc in commands.extension_reconcilers] == ["litestar", "events", "adk"]
        commands.upgrade()
        with sync_cfg.provide_session() as driver:
            driver.execute("COMMIT")
            tables = {row["table_name"] for row in driver.data_dictionary.get_tables(driver)}
            assert {"runner_sessions", "runner_events", "adk_session", "adk_memory"} <= tables
        commands.downgrade("base")
        with sync_cfg.provide_session() as driver:
            driver.execute("COMMIT")
            tables_after = {row["table_name"] for row in driver.data_dictionary.get_tables(driver)}
            assert not ({"runner_sessions", "runner_events", "adk_session", "adk_memory"} & tables_after)
    finally:
        sync_cfg.close()


@pytest.mark.parametrize(
    "case",
    [
        "missing_config",
        "missing_driver",
        "invalid_mode",
        "sync_async_mismatch",
        "wrong_adapter",
    ],
)
def test_sqlite_reconcile_error_semantics(case: str) -> None:
    """Verify SQLite reconciler raises `MigrationError` on missing context fields, mode mismatch, or bad mode."""
    sync_cfg = SQLiteSyncConfig(database=":memory:", extension_config={"litestar": {"session_table": "s"}})
    async_cfg = SQLiteAsyncConfig(database=":memory:", extension_config={"litestar": {"session_table": "s"}})
    sync_driver = SQLiteSyncDriver(_RecordingSyncConn())
    async_driver = SQLiteAsyncDriver(_RecordingAsyncConn())

    if case == "missing_config":
        ctx = MigrationContext(config=None, driver=sync_driver)
    elif case == "missing_driver":
        ctx = MigrationContext(config=sync_cfg, driver=None)
    elif case == "invalid_mode":
        ctx = MigrationContext(config=sync_cfg, driver=sync_driver, extension_config={"migration_mode": "bogus"})
    elif case == "sync_async_mismatch":
        ctx = MigrationContext(config=async_cfg, driver=sync_driver)
    else:
        mysql_cfg = MySQLSyncConfig(url="mysql://user:pw@127.0.0.1:3306/app")
        ctx = MigrationContext(config=mysql_cfg, driver=sync_driver)

    with pytest.raises(MigrationError):
        sqlite_reconcile.reconcile(ctx)
    with pytest.raises(MigrationError):
        sqlite_reconcile.downgrade(ctx)

    with pytest.raises(MigrationError):
        sqlite_reconcile.reconcile(MigrationContext(config=sync_cfg, driver=async_driver))


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


@pytest.mark.parametrize(
    "slotted_cls",
    [
        MySQLSyncConfig,
        MySQLAsyncConfig,
        MySQLSyncDriver,
        MySQLAsyncDriver,
        MySQLSyncConnectionContext,
        MySQLAsyncConnectionContext,
        MySQLSyncSessionContext,
        MySQLAsyncSessionContext,
    ],
)
def test_mysql_adapter_classes_declare_slots_and_typed_surface(slotted_cls: type[Any]) -> None:
    """Verify MySQL adapter configs, drivers, and session/connection contexts declare `__slots__`."""
    assert "__slots__" in slotted_cls.__dict__
    assert MySQLConnectionParams.__required_keys__ == frozenset()
    assert {"host", "port", "user", "password", "database", "unix_socket", "charset", "ssl", "autocommit"} <= set(
        MySQLConnectionParams.__optional_keys__
    )
    assert {"min_size", "max_size", "pool_recycle", "share_pool"} <= set(MySQLPoolParams.__optional_keys__)
    assert {"reset_session_on_release", "use_pure_python_driver", "on_connection_create"} <= set(
        MySQLDriverFeatures.__optional_keys__
    )


@pytest.mark.parametrize(
    ("config_factory", "expected_memory_cls", "expected_artifact_cls"),
    [
        (
            lambda: MySQLSyncConfig(url="mysql://user:pw@127.0.0.1:3306/appdb", share_pool=False),
            MySQLSyncADKMemoryStore,
            MySQLSyncADKArtifactStore,
        ),
        (
            lambda: MySQLAsyncConfig(url="mysql://user:pw@127.0.0.1:3306/appdb", share_pool=False),
            MySQLAsyncADKMemoryStore,
            MySQLAsyncADKArtifactStore,
        ),
    ],
)
def test_mysql_resolves_adk_memory_and_artifact_store_classes(
    config_factory: Any,
    expected_memory_cls: type[Any],
    expected_artifact_cls: type[Any],
) -> None:
    """Verify `resolve_extension_store_class` resolves lazy MySQL ADK memory and artifact store classes."""
    cfg = config_factory()
    assert (
        resolve_extension_store_class(
            cfg,
            extension="adk",
            store_attr="adk_memory_store_class",
            store_suffix="ADKMemoryStore",
        )
        is expected_memory_cls
    )
    assert (
        resolve_extension_store_class(
            cfg,
            extension="adk",
            store_attr="adk_artifact_store_class",
            store_suffix="ADKArtifactStore",
        )
        is expected_artifact_cls
    )


@pytest.mark.anyio
async def test_mysql_session_context_isolation_and_mark_closed() -> None:
    """Verify sync/async MySQL session contexts yield distinct drivers and mark them closed on exit."""
    sync_conn = _RecordingSyncConn()
    sync_cfg = MySQLSyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=sync_conn,
    )
    with sync_cfg.provide_session("primary", timeout=5) as d1, sync_cfg.provide_session("primary") as d2:
        assert d1 is not d2
        assert isinstance(d1, MySQLSyncDriver)
        assert isinstance(d2, MySQLSyncDriver)
        assert d1.native_driver is sync_conn
        assert d2.native_driver is sync_conn
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = d1.connection.execute("SELECT 1")
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = d2.connection.execute("SELECT 1")

    async_conn = _RecordingAsyncConn()
    async_cfg = MySQLAsyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=async_conn,
    )
    async with async_cfg.provide_session("primary") as ad1, async_cfg.provide_session("primary") as ad2:
        assert ad1 is not ad2
        assert isinstance(ad1, MySQLAsyncDriver)
        assert isinstance(ad2, MySQLAsyncDriver)
        assert ad1.native_driver is async_conn
        assert ad2.native_driver is async_conn
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = ad1.connection.execute("SELECT 1")
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = ad2.connection.execute("SELECT 1")


@pytest.mark.anyio
async def test_mysql_reset_session_on_release_and_connection_hooks() -> None:
    """Verify `reset_session_on_release=True` rolls back uncommitted state, runs `RESET CONNECTION`, and invokes hooks."""
    sync_conn = _RecordingSyncConn()
    sync_hooks: list[Any] = []
    sync_cfg = MySQLSyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=sync_conn,
        reset_session_on_release=True,
        driver_features={"on_connection_create": sync_hooks.append},
    )
    with sync_cfg.provide_session() as drv:
        drv.begin()
        assert sync_conn.in_transaction is True
    assert len(sync_hooks) == 1
    assert sync_hooks[0] is sync_conn
    assert sync_conn.in_transaction is False
    assert "rollback" in sync_conn.tx_events
    assert ("execute", "RESET CONNECTION", None) in sync_conn.calls
    with sync_cfg.provide_connection() as conn:
        assert conn is sync_conn
    assert len(sync_hooks) == 1
    assert sync_conn.calls.count(("execute", "RESET CONNECTION", None)) == 2

    async_conn = _RecordingAsyncConn()
    async_hooks: list[Any] = []
    async_cfg = MySQLAsyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=async_conn,
        reset_session_on_release=True,
        driver_features={"on_connection_create": async_hooks.append},
    )
    async with async_cfg.provide_session() as adrv:
        await adrv.begin()
        assert async_conn.in_transaction is True
    assert len(async_hooks) == 1
    assert async_hooks[0] is async_conn
    assert async_conn.in_transaction is False
    assert "rollback" in async_conn.tx_events
    assert ("execute", "RESET CONNECTION", None) in async_conn.calls
    async with async_cfg.provide_connection() as aconn:
        assert aconn is async_conn
    assert len(async_hooks) == 1
    assert async_conn.calls.count(("execute", "RESET CONNECTION", None)) == 2


@pytest.mark.parametrize("config_cls", [MySQLSyncConfig, MySQLAsyncConfig])
def test_mysql_replicas_reject_boolean_shorthand(config_cls: type[Any]) -> None:
    """Verify MySQL adapters reject `replicas=True` shorthand with `ImproperConfigurationError`."""
    with pytest.raises(ImproperConfigurationError, match="explicit replica"):
        config_cls(url="mysql://user:pw@127.0.0.1:3306/appdb", replicas=True, share_pool=False)


@pytest.mark.anyio
async def test_mysql_replicas_routing_and_replica_session() -> None:
    """Verify MySQL `replicas=` routes reads to replicas and writes to primary for sync and async configs."""
    pri_sync_conn = _RecordingSyncConn(rows=[{"src": "primary"}], columns=["src"])
    rep_sync_conn = _RecordingSyncConn(rows=[{"src": "replica"}], columns=["src"])
    sync_cfg = MySQLSyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=pri_sync_conn,
        replicas=[{"host": "127.0.0.2", "port": 3307}],
    )
    assert sync_cfg.has_replicas is True
    replica_cfg = sync_cfg.replica_groups["default"].specs[0].config
    assert replica_cfg.driver_config.parse_dsn_params()["host"] == "127.0.0.2"
    assert replica_cfg.driver_config.parse_dsn_params()["port"] == 3307
    replica_cfg.connection_instance = cast("Any", rep_sync_conn)

    with sync_cfg.provide_replica_session() as rep_drv:
        assert rep_drv.select("SELECT 'replica' AS src") == [{"src": "replica"}]
    routing = sync_cfg.provide_routing_session()
    assert routing.select("SELECT src FROM notes") == [{"src": "replica"}]
    routing.execute("INSERT INTO notes (id) VALUES (1)")
    assert any(call[1] == "SELECT src FROM notes" for call in rep_sync_conn.calls)
    assert any(call[1] == "INSERT INTO notes (id) VALUES (1)" for call in pri_sync_conn.calls)

    pri_async_conn = _RecordingAsyncConn(rows=[{"src": "primary"}], columns=["src"])
    rep_async_conn = _RecordingAsyncConn(rows=[{"src": "replica"}], columns=["src"])
    async_cfg = MySQLAsyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=pri_async_conn,
        replicas=["mysql://user:pw@127.0.0.3:3308/appdb"],
    )
    assert async_cfg.has_replicas is True
    async_replica_cfg = async_cfg.replica_groups["default"].specs[0].config
    assert async_replica_cfg.driver_config.parse_dsn_params()["host"] == "127.0.0.3"
    assert async_replica_cfg.driver_config.parse_dsn_params()["port"] == 3308
    async_replica_cfg.connection_instance = cast("Any", rep_async_conn)

    async with async_cfg.provide_replica_session() as arep_drv:
        assert await arep_drv.select("SELECT 'replica' AS src") == [{"src": "replica"}]
    arouting = async_cfg.provide_routing_session()
    assert await arouting.select("SELECT src FROM notes") == [{"src": "replica"}]
    await arouting.execute("INSERT INTO notes (id) VALUES (2)")
    assert any(call[1] == "SELECT src FROM notes" for call in rep_async_conn.calls)
    assert any(call[1] == "INSERT INTO notes (id) VALUES (2)" for call in pri_async_conn.calls)


@pytest.mark.anyio
async def test_mysql_session_exit_closes_streams_when_rollback_raises_and_chains_notes() -> None:
    """Verify MySQL session exit closes streams even if rollback raises and chains notes via `add_note`."""

    class _TrackingStream:
        def __init__(self) -> None:
            self.closed = False

        def _close_for_session_exit(self, *, error: bool = False) -> None:
            _ = error
            self.closed = True

        async def _aclose_for_session_exit(self, *, error: bool = False) -> None:
            _ = error
            self.closed = True

        def _mark_session_closed(self) -> None:
            self.closed = True

    class _FailingSyncRollbackConn(_RecordingSyncConn):
        def rollback(self) -> None:
            raise RuntimeError("sync rollback failure")

    class _FailingAsyncRollbackConn(_RecordingAsyncConn):
        async def rollback(self) -> None:
            raise RuntimeError("async rollback failure")

    sync_conn = _FailingSyncRollbackConn()
    sync_cfg = MySQLSyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=sync_conn,
        reset_session_on_release=True,
    )
    sync_stream = _TrackingStream()
    with pytest.raises(ValueError, match="body boom") as sync_exc_info, sync_cfg.provide_session() as s_drv:
        s_drv._open_streams.append(sync_stream)
        s_drv.begin()
        raise ValueError("body boom")
    assert sync_stream.closed is True
    assert any("sync rollback failure" in note for note in (sync_exc_info.value.__notes__ or []))
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = s_drv.connection.execute("SELECT 1")

    async_conn = _FailingAsyncRollbackConn()
    async_cfg = MySQLAsyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=async_conn,
        reset_session_on_release=True,
    )
    async_stream = _TrackingStream()
    with pytest.raises(ValueError, match="async body boom") as async_exc_info:
        async with async_cfg.provide_session() as a_drv:
            a_drv._open_streams.append(async_stream)
            await a_drv.begin()
            raise ValueError("async body boom")
    assert async_stream.closed is True
    assert any("async rollback failure" in note for note in (async_exc_info.value.__notes__ or []))
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = a_drv.connection.execute("SELECT 1")


@pytest.mark.anyio
async def test_mysql_enter_failure_releases_lease_and_marks_driver_closed() -> None:
    """Verify connection/hook/prepare failures during `__enter__`/`__aenter__` release leases and close drivers."""
    events: list[str] = []

    class _FakeSyncPool:
        @contextlib.contextmanager
        def get_connection(self) -> Any:
            conn = _RecordingSyncConn()
            events.append("sync_acquire")
            try:
                yield conn
            finally:
                events.append("sync_release")

    class _FakeAsyncPool:
        @contextlib.asynccontextmanager
        async def get_connection(self) -> Any:
            conn = _RecordingAsyncConn()
            events.append("async_acquire")
            try:
                yield conn
            finally:
                events.append("async_release")

    def _fail_connect(_conn: Any) -> None:
        raise DatabaseConnectionError("Unknown MySQL server host 'bad-host'")

    sync_cfg = MySQLSyncConfig(
        url="mysql://user:pw@bad-host:3306/appdb",
        share_pool=False,
        connection_instance=_FakeSyncPool(),
        driver_features={"on_connection_create": _fail_connect},
    )
    with pytest.raises(DatabaseConnectionError, match="bad-host"), sync_cfg.provide_session():
        pass
    assert events == ["sync_acquire", "sync_release"]

    constructed_drivers: list[MySQLSyncDriver] = []

    class _TrackingSyncConfig(MySQLSyncConfig):
        def _prepare_driver(self, driver: MySQLSyncDriver) -> MySQLSyncDriver:
            constructed_drivers.append(driver)
            raise PermissionDeniedError("Access denied for user 'svc'")

    events.clear()
    sync_prepare_cfg = _TrackingSyncConfig(
        url="mysql://svc:bad@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=_FakeSyncPool(),
    )
    with pytest.raises(PermissionDeniedError, match="Access denied"), sync_prepare_cfg.provide_session():
        pass
    assert events == ["sync_acquire", "sync_release"]
    assert len(constructed_drivers) == 1
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = constructed_drivers[0].connection.execute("SELECT 1")

    events.clear()
    async_cfg = MySQLAsyncConfig(
        url="mysql://user:pw@bad-host:3306/appdb",
        share_pool=False,
        connection_instance=_FakeAsyncPool(),
        driver_features={"on_connection_create": _fail_connect},
    )
    with pytest.raises(DatabaseConnectionError, match="bad-host"):
        async with async_cfg.provide_session():
            pass
    assert events == ["async_acquire", "async_release"]

    constructed_async_drivers: list[MySQLAsyncDriver] = []

    class _TrackingAsyncConfig(MySQLAsyncConfig):
        def _prepare_driver(self, driver: MySQLAsyncDriver) -> MySQLAsyncDriver:
            constructed_async_drivers.append(driver)
            raise PermissionDeniedError("Access denied for user 'svc'")

    events.clear()
    async_prepare_cfg = _TrackingAsyncConfig(
        url="mysql://svc:bad@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=_FakeAsyncPool(),
    )
    with pytest.raises(PermissionDeniedError, match="Access denied"):
        async with async_prepare_cfg.provide_session():
            pass
    assert events == ["async_acquire", "async_release"]
    assert len(constructed_async_drivers) == 1
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = constructed_async_drivers[0].connection.execute("SELECT 1")


class _MySQLADKScriptedConn:
    """Query-routing recording MySQL connection double for statement-shape and transaction boundary tests."""

    def __init__(self, handler: Any) -> None:
        self._closed = False
        self._in_tx = False
        self._handler = handler
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
        res = self._handler("execute", sql, parameters)
        return int(res) if res is not None else 1

    def execute_many(self, sql: str, param_sets: Any = None, **kwargs: Any) -> int:
        _ = kwargs
        batch = list(param_sets or [])
        self.calls.append(("execute_many", sql, batch))
        res = self._handler("execute_many", sql, batch)
        return int(res) if res is not None else len(batch)

    def execute_script(self, sql: str, **kwargs: Any) -> int:
        _ = kwargs
        self.scripts.append(sql)
        return 1

    def execute_with_description(self, sql: str, parameters: Any = None, **kwargs: Any) -> tuple[list[Any], list[str]]:
        _ = kwargs
        self.calls.append(("execute_with_description", sql, parameters))
        return cast("tuple[list[Any], list[str]]", self._handler("execute_with_description", sql, parameters))

    def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        _ = kwargs
        self.calls.append(("select", sql, parameters))
        return list(self._handler("select", sql, parameters) or [])

    def select_one(self, sql: str, parameters: Any = None, **kwargs: Any) -> dict[str, Any] | None:
        _ = kwargs
        self.calls.append(("select_one", sql, parameters))
        return cast("dict[str, Any] | None", self._handler("select_one", sql, parameters))

    def begin(self) -> None:
        self._in_tx = True
        self.tx_events.append("begin")

    def commit(self) -> None:
        self._in_tx = False
        self.tx_events.append("commit")

    def rollback(self) -> None:
        self._in_tx = False
        self.tx_events.append("rollback")


class _MySQLADKAsyncScriptedConn:
    """Async query-routing recording MySQL connection double for statement-shape and transaction tests."""

    def __init__(self, handler: Any) -> None:
        self._closed = False
        self._in_tx = False
        self._handler = handler
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
        res = self._handler("execute", sql, parameters)
        return int(res) if res is not None else 1

    async def execute_many(self, sql: str, param_sets: Any = None, **kwargs: Any) -> int:
        _ = kwargs
        batch = list(param_sets or [])
        self.calls.append(("execute_many", sql, batch))
        res = self._handler("execute_many", sql, batch)
        return int(res) if res is not None else len(batch)

    async def execute_script(self, sql: str, **kwargs: Any) -> int:
        _ = kwargs
        self.scripts.append(sql)
        return 1

    async def execute_with_description(
        self, sql: str, parameters: Any = None, **kwargs: Any
    ) -> tuple[list[Any], list[str]]:
        _ = kwargs
        self.calls.append(("execute_with_description", sql, parameters))
        return cast("tuple[list[Any], list[str]]", self._handler("execute_with_description", sql, parameters))

    async def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        _ = kwargs
        self.calls.append(("select", sql, parameters))
        return list(self._handler("select", sql, parameters) or [])

    async def select_one(self, sql: str, parameters: Any = None, **kwargs: Any) -> dict[str, Any] | None:
        _ = kwargs
        self.calls.append(("select_one", sql, parameters))
        return cast("dict[str, Any] | None", self._handler("select_one", sql, parameters))

    async def begin(self) -> None:
        self._in_tx = True
        self.tx_events.append("begin")

    async def commit(self) -> None:
        self._in_tx = False
        self.tx_events.append("commit")

    async def rollback(self) -> None:
        self._in_tx = False
        self.tx_events.append("rollback")


def test_mysql_adk_upstream_bases_config_slots_and_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MySQL ADK upstream bases, deleted fallbacks, aligned config keys, slots, and MissingDependencyError."""
    adk_module = import_module("vertebra.extensions.sqlspec.adapters.mysql.adk")
    assert not hasattr(adk_module, "_FallbackADKMemoryStoreCommon")
    assert not hasattr(adk_module, "_FallbackADKArtifactStoreCommon")
    assert not hasattr(adk_module, "_rank_memory_records")

    assert issubclass(MySQLSyncADKStore, BaseSyncADKStore)
    assert issubclass(MySQLAsyncADKStore, BaseAsyncADKStore)
    assert issubclass(MySQLSyncADKMemoryStore, BaseSyncADKMemoryStore)
    assert issubclass(MySQLAsyncADKMemoryStore, BaseAsyncADKMemoryStore)
    assert issubclass(MySQLSyncADKArtifactStore, BaseSyncADKArtifactStore)
    assert issubclass(MySQLAsyncADKArtifactStore, BaseAsyncADKArtifactStore)

    for store_cls in (
        MySQLSyncADKStore,
        MySQLAsyncADKStore,
        MySQLSyncADKMemoryStore,
        MySQLAsyncADKMemoryStore,
        MySQLSyncADKArtifactStore,
        MySQLAsyncADKArtifactStore,
    ):
        assert "__slots__" in store_cls.__dict__

    expected_keys = {
        "use_vector_type",
        "vector_dimensions",
        "memory_table",
        "artifact_table",
        "enable_memory",
        "memory_use_fts",
        "memory_max_results",
        "memory_retention_days",
        "owner_id_column",
        "enable_event_generated_columns",
        "enable_covering_indexes",
        "session_table_options",
        "events_table_options",
        "app_state_table_options",
        "user_state_table_options",
        "memory_table_options",
    }
    config_keys = set(MySQLADKConfig.__optional_keys__) | set(ADKConfig.__optional_keys__)
    assert expected_keys <= config_keys

    adk_file = Path(adk_module.__file__ or "")
    spec = importlib.util.spec_from_file_location("_test_mysql_adk_missing_dep", adk_file)
    assert spec is not None
    assert spec.loader is not None
    with monkeypatch.context() as mp:
        for mod_name in list(sys.modules):
            if mod_name == "google.adk" or mod_name.startswith("google.adk."):
                mp.setitem(sys.modules, mod_name, None)
            elif mod_name == "sqlspec.extensions.adk" or mod_name.startswith("sqlspec.extensions.adk."):
                mp.delitem(sys.modules, mod_name, raising=False)
        mp.setitem(sys.modules, "google.adk", None)
        isolated_mod = importlib.util.module_from_spec(spec)
        with pytest.raises(MissingDependencyError) as exc_info:
            spec.loader.exec_module(isolated_mod)
        assert exc_info.value.package == "google-adk"
        assert exc_info.value.install_package == "vertebra-core[adk]"


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_mysql_adk_session_deltas_events_and_transactions(is_async_mode: bool) -> None:
    """Verify MySQL ADK session deltas, merged-state returns, event appends, and transaction rollbacks."""
    now = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    app_state_store: dict[str, Any] = {"app:existing": "yes"}
    user_state_store: dict[str, Any] = {"user:existing": 1}
    session_state_value: dict[str, Any] = {"local": 1}
    missing_session_mode = False
    missing_event_target = False

    def _handler(op: str, sql: str, params: Any) -> Any:
        if op == "execute":
            if "INSERT INTO adk_event" in sql and "SELECT" in sql and missing_event_target:
                return 0
            return 1
        if op == "execute_with_description":
            if "FROM adk_session" in sql:
                if missing_session_mode:
                    return [], ["id", "app_name", "user_id", "state", "create_time", "update_time"]
                return [
                    ("sess-1", "app-1", "user-1", session_state_value, now, now),
                ], ["id", "app_name", "user_id", "state", "create_time", "update_time"]
            if "FROM adk_app_state" in sql:
                return [(app_state_store,)], ["state"]
            if "FROM adk_user_state" in sql:
                return [(user_state_store,)], ["state"]
        return []

    ev_record: Any = {
        "id": "ev-1",
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "invocation_id": "inv-1",
        "timestamp": now,
        "event_data": {"author": "user"},
    }
    bad_ev: Any = {**ev_record, "user_id": "other-user"}

    if not is_async_mode:
        conn = _MySQLADKScriptedConn(_handler)
        cfg = MySQLSyncConfig(
            url="mysql://user:pw@127.0.0.1:3306/appdb",
            share_pool=False,
            connection_instance=conn,
        )
        store = MySQLSyncADKStore(cfg)
        assert not hasattr(store, "__dict__")

        created = store.create_session(
            "sess-1",
            "app-1",
            "user-1",
            {"local": 1},
            app_state_delta={"app:theme": "dark"},
            user_state_delta={"user:lang": "en"},
        )
        assert created["state"] == {
            "local": 1,
            "app:existing": "yes",
            "app:theme": "dark",
            "user:existing": 1,
            "user:lang": "en",
        }
        assert conn.tx_events == ["begin", "commit"]
        assert any("SELECT state FROM adk_app_state WHERE app_name = ? FOR UPDATE" in c[1] for c in conn.calls)
        assert any(
            "SELECT state FROM adk_user_state WHERE app_name = ? AND user_id = ? FOR UPDATE" in c[1] for c in conn.calls
        )

        conn.calls.clear()
        conn.tx_events.clear()
        session_state_value = {"local": 2}
        updated = store.append_event_and_update_state(
            ev_record,
            "app-1",
            "user-1",
            "sess-1",
            {"local": 2},
            app_state_delta={"app:count": 5},
            user_state_delta={"user:tier": "pro"},
        )
        assert updated["state"] == {
            "local": 2,
            "app:existing": "yes",
            "app:count": 5,
            "user:existing": 1,
            "user:tier": "pro",
        }
        assert conn.tx_events == ["begin", "commit"]

        conn.tx_events.clear()
        missing_session_mode = True
        with pytest.raises(ValueError, match="not found during append_event_and_update_state"):
            store.append_event_and_update_state(ev_record, "app-1", "user-1", "sess-1", {})
        assert conn.tx_events == ["begin", "rollback"]
        missing_session_mode = False

        with pytest.raises(ValueError, match="event_record scope"):
            store.append_event_and_update_state(bad_ev, "app-1", "user-1", "sess-1", {})

        conn.calls.clear()
        conn.tx_events.clear()
        store.append_event(ev_record)
        assert any("INSERT INTO adk_event" in c[1] and "FROM adk_session" in c[1] for c in conn.calls)
        assert conn.tx_events == ["commit"]

        conn.tx_events.clear()
        missing_event_target = True
        with pytest.raises(ValueError, match="Session sess-1 not found"):
            store.append_event(ev_record)
        assert "rollback" in conn.tx_events
        assert "commit" not in conn.tx_events
        return

    aconn = _MySQLADKAsyncScriptedConn(_handler)
    acfg = MySQLAsyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=aconn,
    )
    astore = MySQLAsyncADKStore(acfg)
    assert not hasattr(astore, "__dict__")

    created_a = await astore.create_session(
        "sess-1",
        "app-1",
        "user-1",
        {"local": 1},
        app_state_delta={"app:theme": "dark"},
        user_state_delta={"user:lang": "en"},
    )
    assert created_a["state"] == {
        "local": 1,
        "app:existing": "yes",
        "app:theme": "dark",
        "user:existing": 1,
        "user:lang": "en",
    }
    assert aconn.tx_events == ["begin", "commit"]

    aconn.calls.clear()
    aconn.tx_events.clear()
    session_state_value = {"local": 2}
    updated_a = await astore.append_event_and_update_state(
        ev_record,
        "app-1",
        "user-1",
        "sess-1",
        {"local": 2},
        app_state_delta={"app:count": 5},
        user_state_delta={"user:tier": "pro"},
    )
    assert updated_a["state"] == {
        "local": 2,
        "app:existing": "yes",
        "app:count": 5,
        "user:existing": 1,
        "user:tier": "pro",
    }
    assert aconn.tx_events == ["begin", "commit"]

    aconn.tx_events.clear()
    missing_session_mode = True
    with pytest.raises(ValueError, match="not found during append_event_and_update_state"):
        await astore.append_event_and_update_state(ev_record, "app-1", "user-1", "sess-1", {})
    assert aconn.tx_events == ["begin", "rollback"]
    missing_session_mode = False

    with pytest.raises(ValueError, match="event_record scope"):
        await astore.append_event_and_update_state(bad_ev, "app-1", "user-1", "sess-1", {})

    aconn.calls.clear()
    aconn.tx_events.clear()
    await astore.append_event(ev_record)
    assert any("INSERT INTO adk_event" in c[1] and "FROM adk_session" in c[1] for c in aconn.calls)
    assert aconn.tx_events == ["commit"]

    aconn.tx_events.clear()
    missing_event_target = True
    with pytest.raises(ValueError, match="Session sess-1 not found"):
        await astore.append_event(ev_record)
    assert "rollback" in aconn.tx_events
    assert "commit" not in aconn.tx_events


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("server_version", "expected_col", "expected_dist_fn"),
    [
        ("11.7.2-MariaDB", "VECTOR(3)", "VEC_DISTANCE_COSINE"),
        ("9.0.1", "VECTOR(3)", "DISTANCE"),
        ("8.4.3", "JSON", None),
    ],
)
async def test_mysql_adk_memory_vector_guards_sql_ranking_and_owner_scoped_delete(
    is_async_mode: bool,
    server_version: str,
    expected_col: str,
    expected_dist_fn: str | None,
) -> None:
    """Verify MySQL ADK memory vector syntax guards, SQL FTS/vector ordering, and owner-scoped session deletes."""
    now = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    sample_rows = [
        {
            "id": "m1",
            "session_id": "s1",
            "app_name": "app-a",
            "user_id": "u1",
            "scope": "user",
            "event_id": "e1",
            "author": "user",
            "timestamp": now,
            "content_json": '{"text": "first"}',
            "content_text": "postgres migration",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": "[0.1, 0.9, 0.0]",
        },
        {
            "id": "m2",
            "session_id": "s1",
            "app_name": "app-a",
            "user_id": "u1",
            "scope": "user",
            "event_id": "e2",
            "author": "user",
            "timestamp": now,
            "content_json": '{"text": "second"}',
            "content_text": "postgres pool",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": "[0.95, 0.05, 0.0]",
        },
    ]

    def _handler(op: str, sql: str, params: Any) -> Any:
        _ = params
        if op == "execute_with_description":
            if "SELECT VERSION()" in sql:
                return [(server_version,)], ["VERSION()"]
            cols = list(sample_rows[0].keys())
            return [tuple(r[c] for c in cols) for r in sample_rows], cols
        if op == "execute":
            if "DELETE FROM adk_memory" in sql:
                return 2
            return 1
        return []

    ext_cfg = {
        "adk": {
            "use_vector_type": True,
            "vector_dimensions": 3,
            "memory_use_fts": True,
            "memory_max_results": 10,
            "memory_retention_days": 30,
        }
    }

    if not is_async_mode:
        conn = _MySQLADKScriptedConn(_handler)
        cfg = MySQLSyncConfig(
            url="mysql://user:pw@127.0.0.1:3306/appdb",
            share_pool=False,
            connection_instance=conn,
            extension_config=ext_cfg,
        )
        mem = MySQLSyncADKMemoryStore(cfg)
        assert not hasattr(mem, "__dict__")
        mem.create_tables()
        ddl_sql = next(c[1] for c in conn.calls if "CREATE TABLE IF NOT EXISTS adk_memory" in c[1])
        assert expected_col in ddl_sql
        assert "FULLTEXT INDEX" in ddl_sql

        conn.calls.clear()
        vec_hits = mem.search_entries("postgres", "app-a", "u1", limit=5, embedding=[1.0, 0.0, 0.0])
        search_sql = conn.calls[-1][1]
        if expected_dist_fn is not None:
            assert expected_dist_fn in search_sql
            assert "ORDER BY vector_distance ASC LIMIT ?" in search_sql
            assert [h["id"] for h in vec_hits] == ["m1", "m2"]
        else:
            assert "VEC_DISTANCE_COSINE" not in search_sql
            assert "DISTANCE(" not in search_sql
            assert [h["id"] for h in vec_hits] == ["m2", "m1"]

        conn.calls.clear()
        fts_hits = mem.search_entries("postgres", "app-a", "u1", limit=5)
        fts_sql = conn.calls[-1][1]
        assert (
            "ORDER BY MATCH(content_text) AGAINST (? IN NATURAL LANGUAGE MODE) DESC, timestamp DESC LIMIT ?" in fts_sql
        )
        assert [h["id"] for h in fts_hits] == ["m1", "m2"]

        conn.calls.clear()
        deleted = mem.delete_entries_by_session("s1", "app-a", "u1")
        assert deleted == 2
        del_call = next(c for c in conn.calls if c[0] == "execute")
        assert del_call[1] == "DELETE FROM adk_memory WHERE session_id = ? AND app_name = ? AND user_id = ?"
        assert del_call[2] == ("s1", "app-a", "u1")
        return

    aconn = _MySQLADKAsyncScriptedConn(_handler)
    acfg = MySQLAsyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=aconn,
        extension_config=ext_cfg,
    )
    amem = MySQLAsyncADKMemoryStore(acfg)
    assert not hasattr(amem, "__dict__")
    await amem.create_tables()
    ddl_sql_a = next(c[1] for c in aconn.calls if "CREATE TABLE IF NOT EXISTS adk_memory" in c[1])
    assert expected_col in ddl_sql_a
    assert "FULLTEXT INDEX" in ddl_sql_a

    aconn.calls.clear()
    vec_hits_a = await amem.search_entries("postgres", "app-a", "u1", limit=5, embedding=[1.0, 0.0, 0.0])
    search_sql_a = aconn.calls[-1][1]
    if expected_dist_fn is not None:
        assert expected_dist_fn in search_sql_a
        assert "ORDER BY vector_distance ASC LIMIT ?" in search_sql_a
        assert [h["id"] for h in vec_hits_a] == ["m1", "m2"]
    else:
        assert "VEC_DISTANCE_COSINE" not in search_sql_a
        assert "DISTANCE(" not in search_sql_a
        assert [h["id"] for h in vec_hits_a] == ["m2", "m1"]

    aconn.calls.clear()
    fts_hits_a = await amem.search_entries("postgres", "app-a", "u1", limit=5)
    fts_sql_a = aconn.calls[-1][1]
    assert "ORDER BY MATCH(content_text) AGAINST (? IN NATURAL LANGUAGE MODE) DESC, timestamp DESC LIMIT ?" in fts_sql_a
    assert [h["id"] for h in fts_hits_a] == ["m1", "m2"]

    aconn.calls.clear()
    deleted_a = await amem.delete_entries_by_session("s1", "app-a", "u1")
    assert deleted_a == 2
    del_call_a = next(c for c in aconn.calls if c[0] == "execute")
    assert del_call_a[1] == "DELETE FROM adk_memory WHERE session_id = ? AND app_name = ? AND user_id = ?"
    assert del_call_a[2] == ("s1", "app-a", "u1")


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_mysql_adk_artifact_store_statements_and_transactions(is_async_mode: bool) -> None:
    """Verify MySQL ADK artifact store DDL, CRUD statement shapes, transactions, and missing-table handling."""
    now = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    raise_missing = False
    artifact_row = {
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "filename": "report.json",
        "version": 1,
        "mime_type": "application/json",
        "canonical_uri": "memory://artifacts/report_v1.json",
        "custom_metadata": '{"stage": "final"}',
        "size": 128,
        "updated": now,
        "signed_url": "https://example.invalid/signed",
        "created_at": now,
    }

    def _handler(op: str, sql: str, params: Any) -> Any:
        _ = params
        if raise_missing:
            raise RuntimeError("ERROR 1146 (42S02): Table 'appdb.adk_artifacts_custom' doesn't exist")
        if op == "execute_with_description":
            if "SELECT MAX(version)" in sql:
                return [(1,)], ["MAX(version)"]
            if "SELECT DISTINCT filename" in sql:
                return [("report.json",)], ["filename"]
            if "SELECT *" in sql:
                cols = list(artifact_row.keys())
                return [tuple(artifact_row[c] for c in cols)], cols
            cols = [
                "app_name",
                "user_id",
                "session_id",
                "filename",
                "version",
                "mime_type",
                "canonical_uri",
                "custom_metadata",
                "created_at",
            ]
            return [tuple(artifact_row[c] for c in cols)], cols
        return 1

    ref = ArtifactReference(
        uri="memory://artifacts/report_v1.json",
        version=1,
        content_type="application/json",
        size=128,
        updated=now,
        signed_url="https://example.invalid/signed",
    )
    ext_cfg = {"adk": {"artifact_table": "adk_artifacts_custom"}}

    if not is_async_mode:
        conn = _MySQLADKScriptedConn(_handler)
        cfg = MySQLSyncConfig(
            url="mysql://user:pw@127.0.0.1:3306/appdb",
            share_pool=False,
            connection_instance=conn,
            extension_config=ext_cfg,
        )
        art = MySQLSyncADKArtifactStore(cfg)
        assert not hasattr(art, "__dict__")
        art.create_table()
        assert any("CREATE TABLE IF NOT EXISTS adk_artifacts_custom" in c[1] for c in conn.calls)

        art.insert_artifact_reference("app-1", "user-1", "report.json", ref, session_id="sess-1")
        assert any("ON DUPLICATE KEY UPDATE" in c[1] for c in conn.calls)
        fetched_ref = art.get_artifact_reference("app-1", "user-1", "report.json", session_id="sess-1")
        assert fetched_ref is not None and fetched_ref.version == 1
        assert art.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 2
        assert art.list_artifact_keys("app-1", "user-1", session_id="sess-1") == ["report.json"]

        conn.tx_events.clear()
        removed = art.delete_artifact("app-1", "user-1", "report.json", session_id="sess-1")
        assert len(removed) == 1
        assert conn.tx_events == ["begin", "commit"]

        conn.tx_events.clear()
        pruned = art.delete_artifacts_older_than(now, app_name="app-1")
        assert len(pruned) == 1
        assert conn.tx_events == ["begin", "commit"]

        raise_missing = True
        assert art.get_artifact("app-1", "user-1", "report.json") is None
        assert art.list_artifact_keys("app-1", "user-1") == []
        assert art.get_next_version("app-1", "user-1", "report.json") == 0
        return

    aconn = _MySQLADKAsyncScriptedConn(_handler)
    acfg = MySQLAsyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/appdb",
        share_pool=False,
        connection_instance=aconn,
        extension_config=ext_cfg,
    )
    aart = MySQLAsyncADKArtifactStore(acfg)
    assert not hasattr(aart, "__dict__")
    await aart.create_table()
    assert any("CREATE TABLE IF NOT EXISTS adk_artifacts_custom" in c[1] for c in aconn.calls)

    await aart.insert_artifact_reference("app-1", "user-1", "report.json", ref, session_id="sess-1")
    assert any("ON DUPLICATE KEY UPDATE" in c[1] for c in aconn.calls)
    fetched_ref_a = await aart.get_artifact_reference("app-1", "user-1", "report.json", session_id="sess-1")
    assert fetched_ref_a is not None and fetched_ref_a.version == 1
    assert await aart.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 2
    assert await aart.list_artifact_keys("app-1", "user-1", session_id="sess-1") == ["report.json"]

    aconn.tx_events.clear()
    removed_a = await aart.delete_artifact("app-1", "user-1", "report.json", session_id="sess-1")
    assert len(removed_a) == 1
    assert aconn.tx_events == ["begin", "commit"]

    aconn.tx_events.clear()
    pruned_a = await aart.delete_artifacts_older_than(now, app_name="app-1")
    assert len(pruned_a) == 1
    assert aconn.tx_events == ["begin", "commit"]

    raise_missing = True
    assert await aart.get_artifact("app-1", "user-1", "report.json") is None
    assert await aart.list_artifact_keys("app-1", "user-1") == []
    assert await aart.get_next_version("app-1", "user-1", "report.json") == 0


class _RecordingMySQLReconcileSyncConn(_RecordingSyncConn):
    """Recording sync MySQL connection serving catalog table/index metadata for reconciliation tests."""

    def __init__(self, *, existing_indexes: list[dict[str, Any]] | None = None) -> None:
        super().__init__(scalar="8.4.3")
        self.existing_indexes = list(existing_indexes or [])

    def execute_with_description(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[list[Any], list[str]]:
        self.calls.append(("execute_with_description", sql, parameters))
        lowered = sql.lower()
        if "information_schema.tables" in lowered:
            return [], ["schema_name", "table_name"]
        if "information_schema.statistics" in lowered:
            cols = ["index_name", "table_name", "is_unique", "is_primary", "columns"]
            table_param = parameters[-1] if isinstance(parameters, (tuple, list)) and parameters else None
            matched = [row for row in self.existing_indexes if table_param is None or row["table_name"] == table_param]
            return [tuple(row[c] for c in cols) for row in matched], cols
        return [("8.4.3",)], ["version"]


class _RecordingMySQLReconcileAsyncConn(_RecordingAsyncConn):
    """Recording async MySQL connection serving catalog table/index metadata for reconciliation tests."""

    def __init__(self, *, existing_indexes: list[dict[str, Any]] | None = None) -> None:
        super().__init__()
        self.existing_indexes = list(existing_indexes or [])

    async def execute_with_description(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[list[Any], list[str]]:
        self.calls.append(("execute_with_description", sql, parameters))
        lowered = sql.lower()
        if "information_schema.tables" in lowered:
            return [], ["schema_name", "table_name"]
        if "information_schema.statistics" in lowered:
            cols = ["index_name", "table_name", "is_unique", "is_primary", "columns"]
            table_param = parameters[-1] if isinstance(parameters, (tuple, list)) and parameters else None
            matched = [row for row in self.existing_indexes if table_param is None or row["table_name"] == table_param]
            return [tuple(row[c] for c in cols) for row in matched], cols
        return [("8.4.3",)], ["version"]


@pytest.mark.parametrize(
    "store_cls",
    [MySQLEventQueueStore, MySQLLitestarStore],
)
def test_mysql_events_litestar_slots_and_reconcile_hooks(store_cls: type[Any]) -> None:
    """Verify MySQL event/Litestar stores declare `__slots__`, hooks, and lazy ADK import isolation."""
    assert "__slots__" in store_cls.__dict__
    cfg = MySQLSyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/app",
        extension_config={
            "litestar": {"table_options": "ROW_FORMAT=DYNAMIC", "index_options": "USING BTREE"},
            "events": {"queue_table": "app.vtb_events", "table_options": "ENGINE=InnoDB"},
        },
    )
    store = store_cls(cfg)
    assert not hasattr(store, "__dict__")
    targets = store.get_schema_targets()
    assert len(targets) == 1
    drops = store.downgrade_drops()
    assert any("DROP TABLE IF EXISTS" in d.statement for d in drops)

    if store_cls is MySQLEventQueueStore:
        assert store._index_existence_target() == ("app", "vtb_events")
        assert "ENGINE=InnoDB" in store._table_ddl()
        companion_stmt = store.companion_index_statement()
        assert companion_stmt is not None
        assert "ALTER TABLE app.vtb_events ADD INDEX idx_app_vtb_events_channel_status" in companion_stmt
    else:
        companions = store.get_companion_indexes()
        assert len(companions) == 1
        assert isinstance(companions[0], CompanionIndex)
        assert "CREATE INDEX idx_litestar_session_expires_at ON litestar_session (expires_at) USING BTREE" in (
            companions[0].ddl
        )

    assert mysql_reconcile.down is mysql_reconcile.downgrade
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import vertebra.extensions.sqlspec.adapters.mysql.reconcile; "
                "assert 'google.adk' not in sys.modules; "
                "assert 'vertebra.extensions.sqlspec.adapters.mysql.adk' not in sys.modules"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("is_async_mode", "index_already_present"),
    [
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    ],
)
async def test_mysql_reconcile_companion_index_metadata_and_downgrade(
    is_async_mode: bool,
    index_already_present: bool,
) -> None:
    """Verify MySQL reconcile checks `get_indexes()` via `metadata_has_index` and downgrades in reverse order."""
    existing_indexes = (
        [
            {
                "index_name": "idx_vtb_sessions_expires_at",
                "table_name": "vtb_sessions",
                "is_unique": 0,
                "is_primary": 0,
                "columns": "expires_at",
            },
            {
                "index_name": "idx_vtb_events_channel_status",
                "table_name": "vtb_events",
                "is_unique": 0,
                "is_primary": 0,
                "columns": "channel,status,available_at",
            },
        ]
        if index_already_present
        else []
    )
    ext_cfg: dict[str, Any] = {
        "litestar": {"session_table": "vtb_sessions"},
        "events": {"queue_table": "vtb_events", "table_options": "ENGINE=InnoDB"},
    }

    if is_async_mode:
        aconn = _RecordingMySQLReconcileAsyncConn(existing_indexes=existing_indexes)
        acfg = MySQLAsyncConfig(
            url="mysql://user:pw@127.0.0.1:3306/app",
            share_pool=False,
            connection_instance=aconn,
            extension_config=ext_cfg,
        )
        driver = MySQLAsyncDriver(aconn)
        ctx = MigrationContext(config=acfg, driver=driver)
        awaitable = mysql_reconcile.reconcile(ctx)
        assert awaitable is not None
        await awaitable

        executed = [*aconn.scripts, *(c[1] for c in aconn.calls)]
        all_sql = "\n".join(executed)
        assert "CREATE TABLE IF NOT EXISTS vtb_sessions" in all_sql
        assert "CREATE TABLE IF NOT EXISTS vtb_events" in all_sql
        assert "ENGINE=InnoDB" in all_sql
        has_lit_idx = any("CREATE INDEX idx_vtb_sessions_expires_at" in s for s in executed)
        has_ev_idx = any("ALTER TABLE vtb_events ADD INDEX idx_vtb_events_channel_status" in s for s in executed)
        assert has_lit_idx is (not index_already_present)
        assert has_ev_idx is (not index_already_present)

        aconn.scripts.clear()
        aconn.calls.clear()
        down_awaitable = mysql_reconcile.downgrade(ctx)
        assert down_awaitable is not None
        await down_awaitable
        drop_sql = [c[1] for c in aconn.calls if "DROP TABLE" in c[1]]
        assert drop_sql == ["DROP TABLE IF EXISTS vtb_events", "DROP TABLE IF EXISTS vtb_sessions"]
        return

    conn = _RecordingMySQLReconcileSyncConn(existing_indexes=existing_indexes)
    cfg = MySQLSyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/app",
        share_pool=False,
        connection_instance=conn,
        extension_config=ext_cfg,
    )
    sdriver = MySQLSyncDriver(conn)
    sctx = MigrationContext(config=cfg, driver=sdriver)
    assert mysql_reconcile.reconcile(sctx) is None

    executed = [*conn.scripts, *(c[1] for c in conn.calls)]
    all_sql = "\n".join(executed)
    assert "CREATE TABLE IF NOT EXISTS vtb_sessions" in all_sql
    assert "CREATE TABLE IF NOT EXISTS vtb_events" in all_sql
    assert "ENGINE=InnoDB" in all_sql
    has_lit_idx = any("CREATE INDEX idx_vtb_sessions_expires_at" in s for s in executed)
    has_ev_idx = any("ALTER TABLE vtb_events ADD INDEX idx_vtb_events_channel_status" in s for s in executed)
    assert has_lit_idx is (not index_already_present)
    assert has_ev_idx is (not index_already_present)

    conn.scripts.clear()
    conn.calls.clear()
    assert mysql_reconcile.downgrade(sctx) is None
    drop_sql = [c[1] for c in conn.calls if "DROP TABLE" in c[1]]
    assert drop_sql == ["DROP TABLE IF EXISTS vtb_events", "DROP TABLE IF EXISTS vtb_sessions"]


def test_mysql_add_extension_migrations_runner_discovery_and_errors(tmp_path: Path) -> None:
    """Verify `add_extension_migrations` discovers MySQL reconciler and enforces error contracts."""
    migrations_dir = tmp_path / "mysql_migrations"
    migrations_dir.mkdir()
    cfg = MySQLSyncConfig(
        url="mysql://user:pw@127.0.0.1:3306/app",
        migration_config={"script_location": str(migrations_dir)},
    )
    cfg.add_extension_migrations(
        "litestar",
        "vertebra.extensions.sqlspec:adapters/mysql",
        migration_mode="reconcile",
    )
    cfg.add_extension_migrations(
        "events",
        "vertebra.extensions.sqlspec:adapters/mysql",
        migration_mode="reconcile",
    )
    commands = cfg.get_migration_commands()
    assert [desc.name for desc in commands.extension_reconcilers] == ["litestar", "events"]
    assert all(desc.module_path.name == "reconcile.py" for desc in commands.extension_reconcilers)

    conn = _RecordingMySQLReconcileSyncConn()
    driver = MySQLSyncDriver(conn)
    async_cfg = MySQLAsyncConfig(url="mysql://user:pw@127.0.0.1:3306/app")
    async_driver = MySQLAsyncDriver(_RecordingMySQLReconcileAsyncConn())

    for bad_ctx in (
        MigrationContext(config=None, driver=driver),
        MigrationContext(config=cfg, driver=None),
        MigrationContext(config=cfg, driver=driver, extension_config={"migration_mode": "invalid"}),
        MigrationContext(config=async_cfg, driver=driver),
        MigrationContext(config=cfg, driver=async_driver),
        MigrationContext(config=SQLiteSyncConfig(database=":memory:"), driver=driver),
    ):
        with pytest.raises(MigrationError):
            mysql_reconcile.reconcile(bad_ctx)
        with pytest.raises(MigrationError):
            mysql_reconcile.downgrade(bad_ctx)


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


@pytest.mark.parametrize(
    "slotted_cls",
    [
        MSSQLSyncConfig,
        MSSQLAsyncConfig,
        MSSQLSyncDriver,
        MSSQLAsyncDriver,
        MSSQLSyncConnectionContext,
        MSSQLAsyncConnectionContext,
        MSSQLSyncSessionContext,
        MSSQLAsyncSessionContext,
    ],
)
def test_mssql_adapter_classes_declare_slots_and_typed_surface(slotted_cls: type[Any]) -> None:
    """Verify MSSQL adapter configs, drivers, and session/connection contexts declare `__slots__`."""
    assert "__slots__" in slotted_cls.__dict__
    assert MSSQLConnectionParams.__required_keys__ == frozenset()
    assert {
        "host",
        "port",
        "database",
        "user",
        "password",
        "connection_string",
        "driver",
        "encrypt",
        "trust_server_certificate",
        "timeout",
    } <= set(MSSQLConnectionParams.__optional_keys__)
    assert {"min_size", "max_size", "pool_recycle", "share_pool"} <= set(MSSQLPoolParams.__optional_keys__)
    assert {
        "json_serializer",
        "json_deserializer",
        "enable_events",
        "events_backend",
        "on_connection_create",
        "reset_session_on_release",
    } <= set(MSSQLDriverFeatures.__optional_keys__)


@pytest.mark.parametrize(
    ("config_factory", "expected_memory_cls", "expected_artifact_cls"),
    [
        (
            lambda: MSSQLSyncConfig(url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb", share_pool=False),
            MSSQLSyncADKMemoryStore,
            MSSQLSyncADKArtifactStore,
        ),
        (
            lambda: MSSQLAsyncConfig(url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb", share_pool=False),
            MSSQLAsyncADKMemoryStore,
            MSSQLAsyncADKArtifactStore,
        ),
    ],
)
def test_mssql_resolves_adk_memory_and_artifact_store_classes(
    config_factory: Any,
    expected_memory_cls: type[Any],
    expected_artifact_cls: type[Any],
) -> None:
    """Verify `resolve_extension_store_class` resolves lazy MSSQL ADK memory and artifact store classes."""
    cfg = config_factory()
    assert (
        resolve_extension_store_class(
            cfg,
            extension="adk",
            store_attr="adk_memory_store_class",
            store_suffix="ADKMemoryStore",
        )
        is expected_memory_cls
    )
    assert (
        resolve_extension_store_class(
            cfg,
            extension="adk",
            store_attr="adk_artifact_store_class",
            store_suffix="ADKArtifactStore",
        )
        is expected_artifact_cls
    )


@pytest.mark.anyio
async def test_mssql_session_context_isolation_streams_and_mark_closed() -> None:
    """Verify sync/async MSSQL session contexts yield distinct drivers, close open streams, and mark closed."""

    class _TrackingStream:
        def __init__(self) -> None:
            self.closed = False

        def _close_for_session_exit(self, *, error: bool = False) -> None:
            _ = error
            self.closed = True

        async def _aclose_for_session_exit(self, *, error: bool = False) -> None:
            _ = error
            self.closed = True

    sync_conn = _RecordingSyncConn()
    sync_cfg = MSSQLSyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=sync_conn,
    )
    sync_stream = _TrackingStream()
    with sync_cfg.provide_session("primary", timeout=5) as d1, sync_cfg.provide_session("primary") as d2:
        assert d1 is not d2
        assert isinstance(d1, MSSQLSyncDriver)
        assert isinstance(d2, MSSQLSyncDriver)
        assert d1.native_driver is sync_conn
        assert d2.native_driver is sync_conn
        d1._open_streams.append(sync_stream)
    assert sync_stream.closed is True
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = d1.connection.execute("SELECT 1")
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = d2.connection.execute("SELECT 1")

    async_conn = _RecordingAsyncConn()
    async_cfg = MSSQLAsyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=async_conn,
    )
    async_stream = _TrackingStream()
    async with async_cfg.provide_session("primary") as ad1, async_cfg.provide_session("primary") as ad2:
        assert ad1 is not ad2
        assert isinstance(ad1, MSSQLAsyncDriver)
        assert isinstance(ad2, MSSQLAsyncDriver)
        assert ad1.native_driver is async_conn
        assert ad2.native_driver is async_conn
        ad1._open_streams.append(async_stream)
    assert async_stream.closed is True
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = ad1.connection.execute("SELECT 1")
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = ad2.connection.execute("SELECT 1")


@pytest.mark.anyio
async def test_mssql_reset_session_on_release_and_connection_hooks() -> None:
    """Verify `reset_session_on_release=True` rolls back state, executes `sp_reset_connection`, and runs hooks."""
    sync_conn = _RecordingSyncConn()
    sync_hooks: list[Any] = []
    sync_cfg = MSSQLSyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=sync_conn,
        reset_session_on_release=True,
        driver_features={"on_connection_create": sync_hooks.append},
    )
    with sync_cfg.provide_session() as drv:
        drv.begin()
        assert sync_conn.in_transaction is True
    assert len(sync_hooks) == 1
    assert sync_hooks[0] is sync_conn
    assert sync_conn.in_transaction is False
    assert "rollback" in sync_conn.tx_events
    assert ("execute", "EXEC sp_reset_connection", None) in sync_conn.calls
    with sync_cfg.provide_connection() as conn:
        assert conn is sync_conn
    assert len(sync_hooks) == 1
    assert sync_conn.calls.count(("execute", "EXEC sp_reset_connection", None)) == 2

    async_conn = _RecordingAsyncConn()
    async_hooks: list[Any] = []
    async_cfg = MSSQLAsyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=async_conn,
        reset_session_on_release=True,
        driver_features={"on_connection_create": async_hooks.append},
    )
    async with async_cfg.provide_session() as adrv:
        await adrv.begin()
        assert async_conn.in_transaction is True
    assert len(async_hooks) == 1
    assert async_hooks[0] is async_conn
    assert async_conn.in_transaction is False
    assert "rollback" in async_conn.tx_events
    assert ("execute", "EXEC sp_reset_connection", None) in async_conn.calls
    async with async_cfg.provide_connection() as aconn:
        assert aconn is async_conn
    assert len(async_hooks) == 1
    assert async_conn.calls.count(("execute", "EXEC sp_reset_connection", None)) == 2


@pytest.mark.parametrize("config_cls", [MSSQLSyncConfig, MSSQLAsyncConfig])
def test_mssql_replicas_reject_boolean_shorthand(config_cls: type[Any]) -> None:
    """Verify MSSQL adapters reject `replicas=True` shorthand with `ImproperConfigurationError`."""
    with pytest.raises(ImproperConfigurationError, match="explicit replica"):
        config_cls(
            url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
            replicas=True,
            share_pool=False,
        )


@pytest.mark.anyio
async def test_mssql_replicas_routing_and_derive_pool() -> None:
    """Verify MSSQL `replicas=` routes reads/writes and `derive_pool` preserves concrete specialization."""
    pri_sync_conn = _RecordingSyncConn(rows=[{"src": "primary"}], columns=["src"])
    rep_sync_conn = _RecordingSyncConn(rows=[{"src": "replica"}], columns=["src"])
    sync_hooks: list[Any] = []
    sync_hook = sync_hooks.append
    sync_cfg = MSSQLSyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=pri_sync_conn,
        reset_session_on_release=True,
        driver_features={"on_connection_create": sync_hook},
        replicas=[{"host": "127.0.0.2", "port": 1434}],
    )
    assert sync_cfg.has_replicas is True
    replica_cfg = sync_cfg.replica_groups["default"].specs[0].config
    assert type(replica_cfg) is MSSQLSyncConfig
    assert replica_cfg.driver_config.host == "127.0.0.2"
    assert replica_cfg.driver_config.port == 1434
    assert replica_cfg.driver_config.database == "appdb"
    assert replica_cfg.driver_features.get("reset_session_on_release") is True
    assert replica_cfg.driver_features.get("on_connection_create") is sync_hook
    replica_cfg.connection_instance = cast("Any", rep_sync_conn)

    derived_sync = sync_cfg.derive_pool(host="127.0.0.4", port=1436)
    assert type(derived_sync) is MSSQLSyncConfig
    assert derived_sync.driver_config.host == "127.0.0.4"
    assert derived_sync.driver_config.port == 1436
    assert derived_sync.driver_features.get("reset_session_on_release") is True

    with sync_cfg.provide_replica_session() as rep_drv:
        assert rep_drv.select("SELECT 'replica' AS src") == [{"src": "replica"}]
    routing = sync_cfg.provide_routing_session()
    assert routing.select("SELECT src FROM notes") == [{"src": "replica"}]
    routing.execute("INSERT INTO notes (id) VALUES (1)")
    assert any(call[1] == "SELECT src FROM notes" for call in rep_sync_conn.calls)
    assert any(call[1] == "INSERT INTO notes (id) VALUES (1)" for call in pri_sync_conn.calls)

    pri_async_conn = _RecordingAsyncConn(rows=[{"src": "primary"}], columns=["src"])
    rep_async_conn = _RecordingAsyncConn(rows=[{"src": "replica"}], columns=["src"])
    async_cfg = MSSQLAsyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=pri_async_conn,
        reset_session_on_release=True,
        replicas=["mssql://sa:test-token-placeholder@127.0.0.3:1435/appdb"],
    )
    assert async_cfg.has_replicas is True
    async_replica_cfg = async_cfg.replica_groups["default"].specs[0].config
    assert type(async_replica_cfg) is MSSQLAsyncConfig
    assert async_replica_cfg.driver_config.host == "127.0.0.3"
    assert async_replica_cfg.driver_config.port == 1435
    assert async_replica_cfg.driver_features.get("reset_session_on_release") is True
    async_replica_cfg.connection_instance = cast("Any", rep_async_conn)

    derived_async = async_cfg.derive_pool(host="127.0.0.5", port=1437)
    assert type(derived_async) is MSSQLAsyncConfig
    assert derived_async.driver_config.host == "127.0.0.5"
    assert derived_async.driver_config.port == 1437
    assert derived_async.driver_features.get("reset_session_on_release") is True

    async with async_cfg.provide_replica_session() as arep_drv:
        assert await arep_drv.select("SELECT 'replica' AS src") == [{"src": "replica"}]
    arouting = async_cfg.provide_routing_session()
    assert await arouting.select("SELECT src FROM notes") == [{"src": "replica"}]
    await arouting.execute("INSERT INTO notes (id) VALUES (2)")
    assert any(call[1] == "SELECT src FROM notes" for call in rep_async_conn.calls)
    assert any(call[1] == "INSERT INTO notes (id) VALUES (2)" for call in pri_async_conn.calls)


@pytest.mark.anyio
async def test_mssql_session_exit_releases_and_rolls_back_on_savepoint_error() -> None:
    """Verify savepoint/session exit errors roll back uncommitted transactions, release leases, and close drivers."""
    events: list[str] = []

    class _FakeSyncPool:
        def __init__(self, conn: _RecordingSyncConn) -> None:
            self._conn = conn

        @contextlib.contextmanager
        def get_connection(self) -> Any:
            events.append("sync_acquire")
            try:
                yield self._conn
            finally:
                events.append("sync_release")

    class _FakeAsyncPool:
        def __init__(self, conn: _RecordingAsyncConn) -> None:
            self._conn = conn

        @contextlib.asynccontextmanager
        async def get_connection(self) -> Any:
            events.append("async_acquire")
            try:
                yield self._conn
            finally:
                events.append("async_release")

    class _FailingReleaseSyncConn(_RecordingSyncConn):
        def release_savepoint(self, name: str) -> None:
            _ = name
            raise RuntimeError("savepoint release boom")

    class _FailingReleaseAsyncConn(_RecordingAsyncConn):
        async def release_savepoint(self, name: str) -> None:
            _ = name
            raise RuntimeError("async savepoint release boom")

    sync_conn = _FailingReleaseSyncConn()
    sync_cfg = MSSQLSyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=_FakeSyncPool(sync_conn),
        reset_session_on_release=True,
    )
    with pytest.raises(RuntimeError, match="savepoint release boom"), sync_cfg.provide_session() as s_drv:
        s_drv.begin()
        s_drv.create_savepoint("sp_1")
        s_drv.release_savepoint("sp_1")
    assert events == ["sync_acquire", "sync_release"]
    assert sync_conn.in_transaction is False
    assert "rollback" in sync_conn.tx_events
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = s_drv.connection.execute("SELECT 1")

    events.clear()
    async_conn = _FailingReleaseAsyncConn()
    async_cfg = MSSQLAsyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=_FakeAsyncPool(async_conn),
        reset_session_on_release=True,
    )
    with pytest.raises(RuntimeError, match="async savepoint release boom"):
        async with async_cfg.provide_session() as a_drv:
            await a_drv.begin()
            await a_drv.create_savepoint("sp_1")
            await a_drv.release_savepoint("sp_1")
    assert events == ["async_acquire", "async_release"]
    assert async_conn.in_transaction is False
    assert "rollback" in async_conn.tx_events
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = a_drv.connection.execute("SELECT 1")


class _MSSQLADKScriptedConn:
    """Query-routing recording MSSQL connection double for statement-shape and transaction tests."""

    def __init__(self, handler: Any) -> None:
        self._closed = False
        self._in_tx = False
        self._handler = handler
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
        res = self._handler("execute", sql, parameters)
        return int(res) if res is not None else 1

    def execute_many(self, sql: str, param_sets: Any = None, **kwargs: Any) -> int:
        _ = kwargs
        batch = list(param_sets or [])
        self.calls.append(("execute_many", sql, batch))
        res = self._handler("execute_many", sql, batch)
        return int(res) if res is not None else len(batch)

    def execute_script(self, sql: str, **kwargs: Any) -> int:
        _ = kwargs
        self.scripts.append(sql)
        return 1

    def execute_with_description(self, sql: str, parameters: Any = None, **kwargs: Any) -> tuple[list[Any], list[str]]:
        _ = kwargs
        self.calls.append(("execute_with_description", sql, parameters))
        return cast("tuple[list[Any], list[str]]", self._handler("execute_with_description", sql, parameters))

    def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        _ = kwargs
        self.calls.append(("select", sql, parameters))
        return list(self._handler("select", sql, parameters) or [])

    def select_one(self, sql: str, parameters: Any = None, **kwargs: Any) -> dict[str, Any] | None:
        _ = kwargs
        self.calls.append(("select_one", sql, parameters))
        return cast("dict[str, Any] | None", self._handler("select_one", sql, parameters))

    def begin(self) -> None:
        self._in_tx = True
        self.tx_events.append("begin")

    def commit(self) -> None:
        self._in_tx = False
        self.tx_events.append("commit")

    def rollback(self) -> None:
        self._in_tx = False
        self.tx_events.append("rollback")


class _MSSQLADKAsyncScriptedConn:
    """Async query-routing recording MSSQL connection double for statement-shape and transaction tests."""

    def __init__(self, handler: Any) -> None:
        self._closed = False
        self._in_tx = False
        self._handler = handler
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
        res = self._handler("execute", sql, parameters)
        return int(res) if res is not None else 1

    async def execute_many(self, sql: str, param_sets: Any = None, **kwargs: Any) -> int:
        _ = kwargs
        batch = list(param_sets or [])
        self.calls.append(("execute_many", sql, batch))
        res = self._handler("execute_many", sql, batch)
        return int(res) if res is not None else len(batch)

    async def execute_script(self, sql: str, **kwargs: Any) -> int:
        _ = kwargs
        self.scripts.append(sql)
        return 1

    async def execute_with_description(
        self, sql: str, parameters: Any = None, **kwargs: Any
    ) -> tuple[list[Any], list[str]]:
        _ = kwargs
        self.calls.append(("execute_with_description", sql, parameters))
        return cast("tuple[list[Any], list[str]]", self._handler("execute_with_description", sql, parameters))

    async def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> list[dict[str, Any]]:
        _ = kwargs
        self.calls.append(("select", sql, parameters))
        return list(self._handler("select", sql, parameters) or [])

    async def select_one(self, sql: str, parameters: Any = None, **kwargs: Any) -> dict[str, Any] | None:
        _ = kwargs
        self.calls.append(("select_one", sql, parameters))
        return cast("dict[str, Any] | None", self._handler("select_one", sql, parameters))

    async def begin(self) -> None:
        self._in_tx = True
        self.tx_events.append("begin")

    async def commit(self) -> None:
        self._in_tx = False
        self.tx_events.append("commit")

    async def rollback(self) -> None:
        self._in_tx = False
        self.tx_events.append("rollback")


def test_mssql_adk_upstream_bases_config_slots_and_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MSSQL ADK upstream bases, deleted fallbacks, aligned config keys, slots, and MissingDependencyError."""
    adk_module = import_module("vertebra.extensions.sqlspec.adapters.mssql.adk")
    assert not hasattr(adk_module, "_FallbackADKMemoryStoreCommon")
    assert not hasattr(adk_module, "_FallbackADKArtifactStoreCommon")
    assert not hasattr(adk_module, "_rank_memory")

    assert issubclass(MSSQLSyncADKStore, BaseSyncADKStore)
    assert issubclass(MSSQLAsyncADKStore, BaseAsyncADKStore)
    assert issubclass(MSSQLSyncADKMemoryStore, BaseSyncADKMemoryStore)
    assert issubclass(MSSQLAsyncADKMemoryStore, BaseAsyncADKMemoryStore)
    assert issubclass(MSSQLSyncADKArtifactStore, BaseSyncADKArtifactStore)
    assert issubclass(MSSQLAsyncADKArtifactStore, BaseAsyncADKArtifactStore)

    for store_cls in (
        MSSQLSyncADKStore,
        MSSQLAsyncADKStore,
        MSSQLSyncADKMemoryStore,
        MSSQLAsyncADKMemoryStore,
        MSSQLSyncADKArtifactStore,
        MSSQLAsyncADKArtifactStore,
        adk_module._MSSQLADKSessionSQLMixin,
        adk_module._MSSQLADKMemorySQLMixin,
        adk_module._MSSQLADKArtifactSQLMixin,
    ):
        assert "__slots__" in store_cls.__dict__

    expected_keys = {
        "memory_table",
        "artifact_table",
        "enable_memory",
        "memory_use_fts",
        "memory_max_results",
        "memory_retention_days",
        "json_type",
        "native_json",
        "owner_id_column",
    }
    config_keys = set(MSSQLADKConfig.__optional_keys__) | set(ADKConfig.__optional_keys__)
    assert expected_keys <= config_keys

    adk_file = Path(adk_module.__file__ or "")
    spec = importlib.util.spec_from_file_location("_test_mssql_adk_missing_dep", adk_file)
    assert spec is not None
    assert spec.loader is not None
    with monkeypatch.context() as mp:
        for mod_name in list(sys.modules):
            if mod_name == "google.adk" or mod_name.startswith("google.adk."):
                mp.setitem(sys.modules, mod_name, None)
            elif mod_name == "sqlspec.extensions.adk" or mod_name.startswith("sqlspec.extensions.adk."):
                mp.delitem(sys.modules, mod_name, raising=False)
        mp.setitem(sys.modules, "google.adk", None)
        isolated_mod = importlib.util.module_from_spec(spec)
        with pytest.raises(MissingDependencyError) as exc_info:
            spec.loader.exec_module(isolated_mod)
        assert exc_info.value.package == "google-adk"
        assert exc_info.value.install_package == "vertebra-core[adk]"


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_mssql_adk_session_deltas_events_and_transactions(is_async_mode: bool) -> None:
    """Verify MSSQL ADK session deltas, merged-state returns, event appends, and transaction rollbacks."""
    now = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    app_state_store: dict[str, Any] = {"app:existing": "yes"}
    user_state_store: dict[str, Any] = {"user:existing": 1}
    session_state_value: dict[str, Any] = {"local": 1}
    missing_session_mode = False
    missing_event_target = False

    def _handler(op: str, sql: str, params: Any) -> Any:
        _ = params
        if op == "execute":
            if "INSERT INTO [dbo].[adk_event]" in sql and "SELECT" in sql and missing_event_target:
                return 0
            return 1
        if op == "execute_with_description":
            if "FROM [dbo].[adk_session]" in sql:
                if missing_session_mode:
                    return [], ["id", "app_name", "user_id", "state", "create_time", "update_time"]
                return [
                    ("sess-1", "app-1", "user-1", session_state_value, now, now),
                ], ["id", "app_name", "user_id", "state", "create_time", "update_time"]
            if "FROM [dbo].[adk_app_state]" in sql:
                return [(app_state_store,)], ["state"]
            if "FROM [dbo].[adk_user_state]" in sql:
                return [(user_state_store,)], ["state"]
            if "FROM [dbo].[adk_event]" in sql:
                return [
                    ("ev-2", "app-1", "user-1", "sess-1", "inv-2", now + timedelta(seconds=1), '{"n": 2}'),
                    ("ev-1", "app-1", "user-1", "sess-1", "inv-1", now, '{"n": 1}'),
                ], ["id", "app_name", "user_id", "session_id", "invocation_id", "timestamp", "event_data"]
        return []

    ev_record: Any = {
        "id": "ev-1",
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "invocation_id": "inv-1",
        "timestamp": now,
        "event_data": {"author": "user"},
    }
    bad_ev: Any = {**ev_record, "user_id": "other-user"}

    if not is_async_mode:
        conn = _MSSQLADKScriptedConn(_handler)
        cfg = MSSQLSyncConfig(
            url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
            share_pool=False,
            connection_instance=conn,
        )
        store = MSSQLSyncADKStore(cfg)
        assert not hasattr(store, "__dict__")

        created = store.create_session(
            "sess-1",
            "app-1",
            "user-1",
            {"local": 1},
            app_state_delta={"app:theme": "dark"},
            user_state_delta={"user:lang": "en"},
        )
        assert created["state"] == {
            "local": 1,
            "app:existing": "yes",
            "app:theme": "dark",
            "user:existing": 1,
            "user:lang": "en",
        }
        assert conn.tx_events == ["begin", "commit"]
        assert any(
            "SELECT TOP (1) state FROM [dbo].[adk_app_state] WITH (UPDLOCK, HOLDLOCK) WHERE app_name = ?" in c[1]
            for c in conn.calls
        )
        assert any(
            "SELECT TOP (1) state FROM [dbo].[adk_user_state] WITH (UPDLOCK, HOLDLOCK) WHERE app_name = ? AND user_id = ?"
            in c[1]
            for c in conn.calls
        )

        conn.calls.clear()
        conn.tx_events.clear()
        session_state_value = {"local": 2}
        updated = store.append_event_and_update_state(
            ev_record,
            "app-1",
            "user-1",
            "sess-1",
            {"local": 2},
            app_state_delta={"app:count": 5},
            user_state_delta={"user:tier": "pro"},
        )
        assert updated["state"] == {
            "local": 2,
            "app:existing": "yes",
            "app:count": 5,
            "user:existing": 1,
            "user:tier": "pro",
        }
        assert conn.tx_events == ["begin", "commit"]

        conn.tx_events.clear()
        missing_session_mode = True
        with pytest.raises(ValueError, match="not found during append_event_and_update_state"):
            store.append_event_and_update_state(ev_record, "app-1", "user-1", "sess-1", {})
        assert conn.tx_events == ["begin", "rollback"]
        missing_session_mode = False

        with pytest.raises(ValueError, match="event_record scope"):
            store.append_event_and_update_state(bad_ev, "app-1", "user-1", "sess-1", {})

        conn.calls.clear()
        conn.tx_events.clear()
        store.append_event(ev_record)
        assert any("INSERT INTO [dbo].[adk_event]" in c[1] and "FROM [dbo].[adk_session]" in c[1] for c in conn.calls)
        assert conn.tx_events == ["commit"]

        conn.tx_events.clear()
        missing_event_target = True
        with pytest.raises(ValueError, match="Session sess-1 not found"):
            store.append_event(ev_record)
        assert "rollback" in conn.tx_events
        assert "commit" not in conn.tx_events
        missing_event_target = False

        conn.calls.clear()
        events = store.get_events("app-1", "user-1", "sess-1", limit=2)
        assert [ev["id"] for ev in events] == ["ev-1", "ev-2"]
        assert any("JOIN [dbo].[adk_session]" in c[1] and "ORDER BY e.timestamp DESC" in c[1] for c in conn.calls)
        return

    aconn = _MSSQLADKAsyncScriptedConn(_handler)
    acfg = MSSQLAsyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=aconn,
    )
    astore = MSSQLAsyncADKStore(acfg)
    assert not hasattr(astore, "__dict__")

    created_a = await astore.create_session(
        "sess-1",
        "app-1",
        "user-1",
        {"local": 1},
        app_state_delta={"app:theme": "dark"},
        user_state_delta={"user:lang": "en"},
    )
    assert created_a["state"] == {
        "local": 1,
        "app:existing": "yes",
        "app:theme": "dark",
        "user:existing": 1,
        "user:lang": "en",
    }
    assert aconn.tx_events == ["begin", "commit"]

    aconn.calls.clear()
    aconn.tx_events.clear()
    session_state_value = {"local": 2}
    updated_a = await astore.append_event_and_update_state(
        ev_record,
        "app-1",
        "user-1",
        "sess-1",
        {"local": 2},
        app_state_delta={"app:count": 5},
        user_state_delta={"user:tier": "pro"},
    )
    assert updated_a["state"] == {
        "local": 2,
        "app:existing": "yes",
        "app:count": 5,
        "user:existing": 1,
        "user:tier": "pro",
    }
    assert aconn.tx_events == ["begin", "commit"]

    aconn.tx_events.clear()
    missing_session_mode = True
    with pytest.raises(ValueError, match="not found during append_event_and_update_state"):
        await astore.append_event_and_update_state(ev_record, "app-1", "user-1", "sess-1", {})
    assert aconn.tx_events == ["begin", "rollback"]
    missing_session_mode = False

    with pytest.raises(ValueError, match="event_record scope"):
        await astore.append_event_and_update_state(bad_ev, "app-1", "user-1", "sess-1", {})

    aconn.calls.clear()
    aconn.tx_events.clear()
    await astore.append_event(ev_record)
    assert any("INSERT INTO [dbo].[adk_event]" in c[1] and "FROM [dbo].[adk_session]" in c[1] for c in aconn.calls)
    assert aconn.tx_events == ["commit"]

    aconn.tx_events.clear()
    missing_event_target = True
    with pytest.raises(ValueError, match="Session sess-1 not found"):
        await astore.append_event(ev_record)
    assert "rollback" in aconn.tx_events
    assert "commit" not in aconn.tx_events
    missing_event_target = False

    aconn.calls.clear()
    events_a = await astore.get_events("app-1", "user-1", "sess-1", limit=2)
    assert [ev["id"] for ev in events_a] == ["ev-1", "ev-2"]
    assert any("JOIN [dbo].[adk_session]" in c[1] and "ORDER BY e.timestamp DESC" in c[1] for c in aconn.calls)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("adk_options", "expected_json_col"),
    [
        ({"json_type": "JSON"}, "JSON"),
        ({"json_type": "NVARCHAR(MAX)"}, "NVARCHAR(MAX)"),
        ({"native_json": True}, "JSON"),
        ({}, "NVARCHAR(MAX)"),
    ],
)
async def test_mssql_adk_memory_json_type_like_search_and_owner_scoped_delete(
    is_async_mode: bool,
    adk_options: dict[str, Any],
    expected_json_col: str,
) -> None:
    """Verify MSSQL ADK memory DDL json_type detection, LIKE search, embedding error, and owner-scoped delete."""
    now = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    sample_rows = [
        {
            "id": "m1",
            "session_id": "s1",
            "app_name": "app-a",
            "user_id": "u1",
            "scope": "user",
            "event_id": "e1",
            "author": "user",
            "timestamp": now,
            "content_json": '{"text": "first"}',
            "content_text": "sqlserver migration",
            "metadata_json": None,
            "inserted_at": now,
        }
    ]

    def _handler(op: str, sql: str, params: Any) -> Any:
        _ = params
        if op == "execute_with_description":
            if "@@VERSION" in sql or "SERVERPROPERTY" in sql:
                return [("Microsoft SQL Server 2022 (RTM) - 16.0.1000.6",)], ["version"]
            if "sys.indexes" in sql:
                return [], ["table_name", "index_name", "columns", "is_unique", "is_primary"]
            cols = list(sample_rows[0].keys())
            return [tuple(r[c] for c in cols) for r in sample_rows], cols
        if op == "execute":
            if "DELETE FROM [sales].[adk_memory_custom]" in sql:
                return 2
            return 1
        return []

    ext_cfg = {
        "adk": {
            "memory_table": "sales.adk_memory_custom",
            "memory_use_fts": True,
            "memory_max_results": 10,
            "memory_retention_days": 30,
            "create_schema": True,
            **adk_options,
        }
    }

    if not is_async_mode:
        conn = _MSSQLADKScriptedConn(_handler)
        cfg = MSSQLSyncConfig(
            url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
            share_pool=False,
            connection_instance=conn,
            extension_config=ext_cfg,
        )
        mem = MSSQLSyncADKMemoryStore(cfg)
        assert not hasattr(mem, "__dict__")
        mem.create_tables()
        ddl_sql = next(c[1] for c in conn.calls if "CREATE TABLE [sales].[adk_memory_custom]" in c[1])
        assert f"content_json {expected_json_col} NOT NULL" in ddl_sql
        assert f"metadata_json {expected_json_col} NULL" in ddl_sql
        assert "UNIQUE (app_name, user_id, event_id)" in ddl_sql

        conn.calls.clear()
        hits = mem.search_entries("sqlserver", "app-a", "u1", limit=5)
        assert [h["id"] for h in hits] == ["m1"]
        search_call = next(c for c in conn.calls if "LIKE ? ESCAPE N'\\'" in c[1])
        assert "[sales].[adk_memory_custom]" in search_call[1]
        assert mem.search_entries("   ", "app-a", "u1") == []
        assert mem.search_entries("sqlserver", "app-a", "u1", limit=0) == []
        with pytest.raises(ImproperConfigurationError, match="Vector embeddings"):
            mem.search_entries("sqlserver", "app-a", "u1", embedding=[0.1, 0.2])

        conn.calls.clear()
        deleted = mem.delete_entries_by_session("s1", "app-a", "u1")
        assert deleted == 2
        del_call = next(c for c in conn.calls if c[0] == "execute")
        assert (
            del_call[1]
            == "DELETE FROM [sales].[adk_memory_custom] WHERE session_id = ? AND app_name = ? AND user_id = ?"
        )
        assert del_call[2] == ("s1", "app-a", "u1")
        return

    aconn = _MSSQLADKAsyncScriptedConn(_handler)
    acfg = MSSQLAsyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=aconn,
        extension_config=ext_cfg,
    )
    amem = MSSQLAsyncADKMemoryStore(acfg)
    assert not hasattr(amem, "__dict__")
    await amem.create_tables()
    ddl_sql_a = next(c[1] for c in aconn.calls if "CREATE TABLE [sales].[adk_memory_custom]" in c[1])
    assert f"content_json {expected_json_col} NOT NULL" in ddl_sql_a
    assert f"metadata_json {expected_json_col} NULL" in ddl_sql_a
    assert "UNIQUE (app_name, user_id, event_id)" in ddl_sql_a

    aconn.calls.clear()
    hits_a = await amem.search_entries("sqlserver", "app-a", "u1", limit=5)
    assert [h["id"] for h in hits_a] == ["m1"]
    search_call_a = next(c for c in aconn.calls if "LIKE ? ESCAPE N'\\'" in c[1])
    assert "[sales].[adk_memory_custom]" in search_call_a[1]
    assert await amem.search_entries("   ", "app-a", "u1") == []
    assert await amem.search_entries("sqlserver", "app-a", "u1", limit=0) == []
    with pytest.raises(ImproperConfigurationError, match="Vector embeddings"):
        await amem.search_entries("sqlserver", "app-a", "u1", embedding=[0.1, 0.2])

    aconn.calls.clear()
    deleted_a = await amem.delete_entries_by_session("s1", "app-a", "u1")
    assert deleted_a == 2
    del_call_a = next(c for c in aconn.calls if c[0] == "execute")
    assert (
        del_call_a[1] == "DELETE FROM [sales].[adk_memory_custom] WHERE session_id = ? AND app_name = ? AND user_id = ?"
    )
    assert del_call_a[2] == ("s1", "app-a", "u1")


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_mssql_adk_artifact_store_versioning_and_transactions(is_async_mode: bool) -> None:
    """Verify MSSQL ADK artifact store DDL, MERGE statements, versioning, transactions, and missing-table handling."""
    now = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    raise_missing = False
    artifact_row = {
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "filename": "report.json",
        "version": 1,
        "mime_type": "application/json",
        "canonical_uri": "memory://artifacts/report_v1.json",
        "custom_metadata": '{"stage": "final"}',
        "size": 128,
        "updated": now,
        "signed_url": "https://example.invalid/signed",
        "created_at": now,
    }

    def _handler(op: str, sql: str, params: Any) -> Any:
        _ = params
        if raise_missing:
            raise RuntimeError("Msg 208: Invalid object name 'dbo.adk_artifacts_custom'")
        if op == "execute_with_description":
            if "SELECT MAX(version)" in sql:
                return [(1,)], ["max_ver"]
            if "SELECT DISTINCT filename" in sql:
                return [("report.json",)], ["filename"]
            if "SELECT TOP (1) *" in sql:
                cols = list(artifact_row.keys())
                return [tuple(artifact_row[c] for c in cols)], cols
            cols = [
                "app_name",
                "user_id",
                "session_id",
                "filename",
                "version",
                "mime_type",
                "canonical_uri",
                "custom_metadata",
                "created_at",
            ]
            return [tuple(artifact_row[c] for c in cols)], cols
        return 1

    ref = ArtifactReference(
        uri="memory://artifacts/report_v1.json",
        version=1,
        content_type="application/json",
        size=128,
        updated=now,
        signed_url="https://example.invalid/signed",
    )
    ext_cfg = {"adk": {"artifact_table": "adk_artifacts_custom", "json_type": "JSON"}}

    if not is_async_mode:
        conn = _MSSQLADKScriptedConn(_handler)
        cfg = MSSQLSyncConfig(
            url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
            share_pool=False,
            connection_instance=conn,
            extension_config=ext_cfg,
        )
        art = MSSQLSyncADKArtifactStore(cfg)
        assert not hasattr(art, "__dict__")
        art.create_table()
        assert any(
            "CREATE TABLE [dbo].[adk_artifacts_custom]" in c[1] and "custom_metadata JSON NULL" in c[1]
            for c in conn.calls
        )

        art.insert_artifact_reference("app-1", "user-1", "report.json", ref, session_id="sess-1")
        assert any("MERGE INTO [dbo].[adk_artifacts_custom] WITH (HOLDLOCK)" in c[1] for c in conn.calls)
        fetched_ref = art.get_artifact_reference("app-1", "user-1", "report.json", session_id="sess-1")
        assert fetched_ref is not None and fetched_ref.version == 1
        assert any("ORDER BY version DESC" in c[1] for c in conn.calls)
        exact_art = art.get_artifact("app-1", "user-1", "report.json", session_id="sess-1", version=1)
        assert exact_art is not None and exact_art["version"] == 1
        assert art.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 2
        assert art.list_artifact_keys("app-1", "user-1", session_id="sess-1") == ["report.json"]

        conn.tx_events.clear()
        removed = art.delete_artifact("app-1", "user-1", "report.json", session_id="sess-1")
        assert len(removed) == 1
        assert conn.tx_events == ["begin", "commit"]

        conn.tx_events.clear()
        pruned = art.delete_artifacts_older_than(now, app_name="app-1")
        assert len(pruned) == 1
        assert conn.tx_events == ["begin", "commit"]

        raise_missing = True
        assert art.get_artifact("app-1", "user-1", "report.json") is None
        assert art.list_artifact_keys("app-1", "user-1") == []
        assert art.get_next_version("app-1", "user-1", "report.json") == 0
        return

    aconn = _MSSQLADKAsyncScriptedConn(_handler)
    acfg = MSSQLAsyncConfig(
        url="mssql://sa:test-token-placeholder@127.0.0.1:1433/appdb",
        share_pool=False,
        connection_instance=aconn,
        extension_config=ext_cfg,
    )
    aart = MSSQLAsyncADKArtifactStore(acfg)
    assert not hasattr(aart, "__dict__")
    await aart.create_table()
    assert any(
        "CREATE TABLE [dbo].[adk_artifacts_custom]" in c[1] and "custom_metadata JSON NULL" in c[1] for c in aconn.calls
    )

    await aart.insert_artifact_reference("app-1", "user-1", "report.json", ref, session_id="sess-1")
    assert any("MERGE INTO [dbo].[adk_artifacts_custom] WITH (HOLDLOCK)" in c[1] for c in aconn.calls)
    fetched_ref_a = await aart.get_artifact_reference("app-1", "user-1", "report.json", session_id="sess-1")
    assert fetched_ref_a is not None and fetched_ref_a.version == 1
    assert any("ORDER BY version DESC" in c[1] for c in aconn.calls)
    exact_art_a = await aart.get_artifact("app-1", "user-1", "report.json", session_id="sess-1", version=1)
    assert exact_art_a is not None and exact_art_a["version"] == 1
    assert await aart.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 2
    assert await aart.list_artifact_keys("app-1", "user-1", session_id="sess-1") == ["report.json"]

    aconn.tx_events.clear()
    removed_a = await aart.delete_artifact("app-1", "user-1", "report.json", session_id="sess-1")
    assert len(removed_a) == 1
    assert aconn.tx_events == ["begin", "commit"]

    aconn.tx_events.clear()
    pruned_a = await aart.delete_artifacts_older_than(now, app_name="app-1")
    assert len(pruned_a) == 1
    assert aconn.tx_events == ["begin", "commit"]

    raise_missing = True
    assert await aart.get_artifact("app-1", "user-1", "report.json") is None
    assert await aart.list_artifact_keys("app-1", "user-1") == []
    assert await aart.get_next_version("app-1", "user-1", "report.json") == 0


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
    "slotted_cls",
    [
        DuckDBSyncConfig,
        DuckDBAsyncConfig,
        DuckDBSyncDriver,
        DuckDBAsyncDriver,
        DuckDBSyncConnectionContext,
        DuckDBAsyncConnectionContext,
        DuckDBSyncSessionContext,
        DuckDBAsyncSessionContext,
    ],
)
def test_duckdb_adapter_classes_declare_slots_and_typed_surface(slotted_cls: type[Any]) -> None:
    """Verify DuckDB adapter configs, drivers, and session/connection contexts declare `__slots__`."""
    assert "__slots__" in slotted_cls.__dict__
    assert DuckDBConnectionParams.__required_keys__ == frozenset()
    assert {
        "database",
        "read_only",
        "config",
        "memory_limit",
        "threads",
        "temp_directory",
    } <= set(DuckDBConnectionParams.__optional_keys__)
    assert {"min_size", "max_size", "pool_recycle", "share_pool"} <= set(DuckDBPoolParams.__optional_keys__)
    assert {
        "json_serializer",
        "json_deserializer",
        "extensions",
        "secrets",
        "on_connection_create",
        "reset_session_on_release",
        "enable_numpy_arrays",
    } <= set(DuckDBDriverFeatures.__optional_keys__)


@pytest.mark.parametrize(
    ("config_factory", "expected_memory_cls", "expected_artifact_cls"),
    [
        (
            lambda: DuckDBSyncConfig(database=":memory:unit_sync_adk", share_pool=False),
            DuckDBSyncADKMemoryStore,
            DuckDBSyncADKArtifactStore,
        ),
        (
            lambda: DuckDBAsyncConfig(database=":memory:unit_async_adk", share_pool=False),
            DuckDBAsyncADKMemoryStore,
            DuckDBAsyncADKArtifactStore,
        ),
    ],
)
def test_duckdb_resolves_adk_memory_and_artifact_store_classes(
    config_factory: Any,
    expected_memory_cls: type[Any],
    expected_artifact_cls: type[Any],
) -> None:
    """Verify `resolve_extension_store_class` resolves lazy DuckDB ADK memory and artifact store classes."""
    cfg = config_factory()
    assert (
        resolve_extension_store_class(
            cfg,
            extension="adk",
            store_attr="adk_memory_store_class",
            store_suffix="ADKMemoryStore",
        )
        is expected_memory_cls
    )
    assert (
        resolve_extension_store_class(
            cfg,
            extension="adk",
            store_attr="adk_artifact_store_class",
            store_suffix="ADKArtifactStore",
        )
        is expected_artifact_cls
    )
    namespace = cfg.get_signature_namespace()
    assert namespace["DuckDBConnectionParams"] is DuckDBConnectionParams
    assert namespace["DuckDBPoolParams"] is DuckDBPoolParams
    assert namespace["DuckDBDriverFeatures"] is DuckDBDriverFeatures


@pytest.mark.anyio
async def test_duckdb_session_context_isolation_streams_and_mark_closed() -> None:
    """Verify sync/async DuckDB session contexts yield distinct drivers, close open streams, and mark closed."""

    class _TrackingStream:
        def __init__(self) -> None:
            self.closed = False

        def _close_for_session_exit(self, *, error: bool = False) -> None:
            _ = error
            self.closed = True

        async def _aclose_for_session_exit(self, *, error: bool = False) -> None:
            _ = error
            self.closed = True

    sync_conn = _RecordingSyncConn()
    sync_cfg = DuckDBSyncConfig(
        database=":memory:unit_sync_sess",
        share_pool=False,
        connection_instance=sync_conn,
    )
    sync_stream = _TrackingStream()
    with sync_cfg.provide_session("primary", timeout=5) as d1, sync_cfg.provide_session("primary") as d2:
        assert d1 is not d2
        assert isinstance(d1, DuckDBSyncDriver)
        assert isinstance(d2, DuckDBSyncDriver)
        assert d1.native_driver is sync_conn
        assert d2.native_driver is sync_conn
        compiled_copy, copy_params = d1.prepare_copy_to_query(
            "SELECT * FROM events WHERE user_id = ?",
            11,
            destination="/tmp/out.parquet",
        )
        assert "FORMAT PARQUET" in compiled_copy.upper()
        assert copy_params == (11, "/tmp/out.parquet")
        d1._open_streams.append(sync_stream)
    assert sync_stream.closed is True
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = d1.connection.execute("SELECT 1")
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = d2.connection.execute("SELECT 1")

    with sync_cfg.provide_connection("primary", timeout=5) as conn_ref:
        assert conn_ref is sync_conn

    async_conn = _RecordingAsyncConn()
    async_cfg = DuckDBAsyncConfig(
        database=":memory:unit_async_sess",
        share_pool=False,
        connection_instance=async_conn,
    )
    async_stream = _TrackingStream()
    async with async_cfg.provide_session("primary", timeout=5) as ad1, async_cfg.provide_session("primary") as ad2:
        assert ad1 is not ad2
        assert isinstance(ad1, DuckDBAsyncDriver)
        assert isinstance(ad2, DuckDBAsyncDriver)
        assert ad1.native_driver is async_conn
        assert ad2.native_driver is async_conn
        ad1._open_streams.append(async_stream)
    assert async_stream.closed is True
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = ad1.connection.execute("SELECT 1")
    with pytest.raises(SQLSpecImproperConfigurationError, match="already closed"):
        _ = ad2.connection.execute("SELECT 1")

    async with async_cfg.provide_connection("primary", timeout=5) as aconn_ref:
        assert aconn_ref is async_conn


@pytest.mark.anyio
async def test_duckdb_reset_session_on_release_and_hooks() -> None:
    """Verify `reset_session_on_release=True` rolls back, resets settings, and runs extension/secret/create hooks."""
    sync_conn = _RecordingSyncConn()
    sync_hooks: list[Any] = []
    sync_cfg = DuckDBSyncConfig(
        database=":memory:unit_sync_reset",
        share_pool=False,
        connection_instance=sync_conn,
        reset_session_on_release=True,
        driver_features={
            "extensions": ["json"],
            "secrets": [{"name": "s3_test", "secret_type": "s3", "value": {"region": "us-east-1"}}],
            "on_connection_create": sync_hooks.append,
        },
    )
    with sync_cfg.provide_session() as drv:
        drv.begin()
        assert sync_conn.in_transaction is True
    assert len(sync_hooks) == 1
    assert sync_hooks[0] is sync_conn
    assert sync_conn.in_transaction is False
    assert "rollback" in sync_conn.tx_events
    assert ("execute", 'LOAD "json"', None) in sync_conn.calls
    assert any("CREATE OR REPLACE SECRET s3_test" in sql for _, sql, _ in sync_conn.calls)
    assert ("execute", "RESET search_path", None) in sync_conn.calls

    with sync_cfg.provide_connection() as conn:
        assert conn is sync_conn
    assert len(sync_hooks) == 1
    assert sync_conn.calls.count(("execute", "RESET search_path", None)) == 2

    async_conn = _RecordingAsyncConn()
    async_hooks: list[Any] = []
    async_cfg = DuckDBAsyncConfig(
        database=":memory:unit_async_reset",
        share_pool=False,
        connection_instance=async_conn,
        reset_session_on_release=True,
        driver_features={
            "extensions": ["json"],
            "secrets": [{"name": "s3_async_test", "secret_type": "s3", "value": {"region": "us-west-2"}}],
            "on_connection_create": async_hooks.append,
        },
    )
    async with async_cfg.provide_session() as adrv:
        await adrv.begin()
        assert async_conn.in_transaction is True
    assert len(async_hooks) == 1
    assert async_hooks[0] is async_conn
    assert async_conn.in_transaction is False
    assert "rollback" in async_conn.tx_events
    assert ("execute", 'LOAD "json"', None) in async_conn.calls
    assert any("CREATE OR REPLACE SECRET s3_async_test" in sql for _, sql, _ in async_conn.calls)
    assert ("execute", "RESET search_path", None) in async_conn.calls

    async with async_cfg.provide_connection() as aconn:
        assert aconn is async_conn
    assert len(async_hooks) == 1
    assert async_conn.calls.count(("execute", "RESET search_path", None)) == 2


@pytest.mark.parametrize("config_cls", [DuckDBSyncConfig, DuckDBAsyncConfig])
@pytest.mark.parametrize(
    "bad_replicas",
    [
        True,
        [False],
        [":memory:"],
        [":memory:rep_loop"],
        [""],
        [{"threads": 2}],
    ],
)
def test_duckdb_replicas_reject_in_memory_and_boolean_shorthands(
    config_cls: type[Any],
    bad_replicas: Any,
) -> None:
    """Verify DuckDB adapters reject boolean and in-memory `:memory:` replica loops cleanly."""
    with pytest.raises(ImproperConfigurationError, match="DuckDB replicas require"):
        config_cls(
            database=":memory:primary_mem",
            replicas=bad_replicas,
            share_pool=False,
        )


@pytest.mark.parametrize("config_cls", [DuckDBSyncConfig, DuckDBAsyncConfig])
def test_duckdb_replicas_reject_same_file_loop(config_cls: type[Any], tmp_path: Path) -> None:
    """Verify DuckDB adapters reject read replicas pointing at the read-write primary file."""
    pri_file = tmp_path / "primary.duckdb"
    with pytest.raises(ImproperConfigurationError, match="separate snapshot"):
        config_cls(
            database=str(pri_file),
            replicas=[str(pri_file)],
            share_pool=False,
        )


@pytest.mark.anyio
async def test_duckdb_replicas_routing_and_derive_pool(tmp_path: Path) -> None:
    """Verify DuckDB `replicas=` routing, `read_only` preservation, and `derive_pool` specialization."""
    pri_file = tmp_path / "analytics_primary.duckdb"
    rep_file = tmp_path / "analytics_replica.duckdb"
    alt_file = tmp_path / "analytics_derived.duckdb"

    pri_sync_conn = _RecordingSyncConn(rows=[{"src": "primary"}], columns=["src"])
    rep_sync_conn = _RecordingSyncConn(rows=[{"src": "replica"}], columns=["src"])
    sync_hooks: list[Any] = []
    sync_hook = sync_hooks.append
    sync_cfg = DuckDBSyncConfig(
        database=str(pri_file),
        share_pool=False,
        connection_instance=pri_sync_conn,
        reset_session_on_release=True,
        driver_features={"on_connection_create": sync_hook},
        replicas=[str(rep_file)],
    )
    assert sync_cfg.has_replicas is True
    replica_cfg = sync_cfg.replica_groups["default"].specs[0].config
    assert type(replica_cfg) is DuckDBSyncConfig
    assert replica_cfg.driver_config.database == str(rep_file)
    assert replica_cfg.driver_config.read_only is True
    assert replica_cfg.connection_config.get("read_only") is True
    assert replica_cfg.driver_features.get("reset_session_on_release") is True
    assert replica_cfg.driver_features.get("on_connection_create") is sync_hook
    replica_cfg.connection_instance = cast("Any", rep_sync_conn)

    derived_from_replica = replica_cfg.derive_pool(threads=2)
    assert type(derived_from_replica) is DuckDBSyncConfig
    assert derived_from_replica.driver_config.read_only is True
    assert derived_from_replica.driver_config.threads == 2
    assert derived_from_replica.driver_config.database == str(rep_file)

    derived_sync = sync_cfg.derive_pool(database=str(alt_file), read_only=True)
    assert type(derived_sync) is DuckDBSyncConfig
    assert derived_sync.driver_config.database == str(alt_file)
    assert derived_sync.driver_config.read_only is True
    assert derived_sync.driver_features.get("reset_session_on_release") is True

    with sync_cfg.provide_replica_session() as rep_drv:
        assert rep_drv.select("SELECT 'replica' AS src") == [{"src": "replica"}]
    routing = sync_cfg.provide_routing_session()
    assert routing.select("SELECT src FROM events") == [{"src": "replica"}]
    routing.execute("INSERT INTO events (id) VALUES (1)")
    assert any(call[1] == "SELECT src FROM events" for call in rep_sync_conn.calls)
    assert any(call[1] == "INSERT INTO events (id) VALUES (1)" for call in pri_sync_conn.calls)

    pri_async_conn = _RecordingAsyncConn(rows=[{"src": "primary"}], columns=["src"])
    rep_async_conn = _RecordingAsyncConn(rows=[{"src": "replica"}], columns=["src"])
    async_cfg = DuckDBAsyncConfig(
        database=str(pri_file),
        share_pool=False,
        connection_instance=pri_async_conn,
        reset_session_on_release=True,
        replicas=[{"database": str(rep_file)}],
    )
    assert async_cfg.has_replicas is True
    async_replica_cfg = async_cfg.replica_groups["default"].specs[0].config
    assert type(async_replica_cfg) is DuckDBAsyncConfig
    assert async_replica_cfg.driver_config.database == str(rep_file)
    assert async_replica_cfg.driver_config.read_only is True
    assert async_replica_cfg.driver_features.get("reset_session_on_release") is True
    async_replica_cfg.connection_instance = cast("Any", rep_async_conn)

    derived_async = async_replica_cfg.derive_pool(threads=3)
    assert type(derived_async) is DuckDBAsyncConfig
    assert derived_async.driver_config.read_only is True
    assert derived_async.driver_config.threads == 3

    async with async_cfg.provide_replica_session() as arep_drv:
        assert await arep_drv.select("SELECT 'replica' AS src") == [{"src": "replica"}]
    arouting = async_cfg.provide_routing_session()
    assert await arouting.select("SELECT src FROM events") == [{"src": "replica"}]
    await arouting.execute("INSERT INTO events (id) VALUES (2)")
    assert any(call[1] == "SELECT src FROM events" for call in rep_async_conn.calls)
    assert any(call[1] == "INSERT INTO events (id) VALUES (2)" for call in pri_async_conn.calls)


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


def test_duckdb_adk_upstream_bases_config_slots_and_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify DuckDB ADK upstream bases, deleted fallbacks, aligned config keys, slots, and MissingDependencyError."""
    adk_module = import_module("vertebra.extensions.sqlspec.adapters.duckdb.adk")
    assert not hasattr(adk_module, "_FallbackADKMemoryStoreCommon")
    assert not hasattr(adk_module, "_FallbackADKArtifactStoreCommon")
    assert not hasattr(adk_module, "_DuckDBArtifactStoreCommon")
    assert not hasattr(adk_module, "_rank_memory_records")

    assert issubclass(DuckDBSyncADKStore, BaseSyncADKStore)
    assert issubclass(DuckDBAsyncADKStore, BaseAsyncADKStore)
    assert issubclass(DuckDBSyncADKMemoryStore, BaseSyncADKMemoryStore)
    assert issubclass(DuckDBAsyncADKMemoryStore, BaseAsyncADKMemoryStore)
    assert issubclass(DuckDBSyncADKArtifactStore, BaseSyncADKArtifactStore)
    assert issubclass(DuckDBAsyncADKArtifactStore, BaseAsyncADKArtifactStore)

    for store_cls in (
        DuckDBSyncADKStore,
        DuckDBAsyncADKStore,
        DuckDBSyncADKMemoryStore,
        DuckDBAsyncADKMemoryStore,
        DuckDBSyncADKArtifactStore,
        DuckDBAsyncADKArtifactStore,
    ):
        assert "__slots__" in store_cls.__dict__

    expected_keys = {
        "enable_memory",
        "memory_table",
        "artifact_table",
        "use_fts",
        "fts_options",
        "memory_fts_options",
        "enable_vector",
        "vector_dimensions",
        "owner_id_column",
        "enable_event_generated_columns",
        "enable_event_generated_column_indexes",
    }
    config_keys = set(DuckDBADKConfig.__optional_keys__) | set(ADKConfig.__optional_keys__)
    assert expected_keys <= config_keys

    adk_file = Path(adk_module.__file__ or "")
    spec = importlib.util.spec_from_file_location("_test_duckdb_adk_missing_dep", adk_file)
    assert spec is not None
    assert spec.loader is not None
    with monkeypatch.context() as mp:
        for mod_name in list(sys.modules):
            if mod_name == "google.adk" or mod_name.startswith("google.adk."):
                mp.setitem(sys.modules, mod_name, None)
            elif mod_name == "sqlspec.extensions.adk" or mod_name.startswith("sqlspec.extensions.adk."):
                mp.delitem(sys.modules, mod_name, raising=False)
        mp.setitem(sys.modules, "google.adk", None)
        isolated_mod = importlib.util.module_from_spec(spec)
        with pytest.raises(MissingDependencyError) as exc_info:
            spec.loader.exec_module(isolated_mod)
        assert exc_info.value.package == "google-adk"
        assert exc_info.value.install_package == "vertebra-core[adk]"


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_duckdb_adk_session_deltas_events_and_missing_tables(is_async_mode: bool) -> None:
    """Verify DuckDB ADK session deltas, merged state returns, event limits, and missing-table behavior."""
    adk_ext: dict[str, Any] = {
        "adk": {
            "enable_event_generated_columns": True,
            "enable_event_generated_column_indexes": True,
        }
    }
    ts1 = datetime(2026, 11, 17, 4, 0, tzinfo=UTC)
    ts2 = ts1 + timedelta(seconds=5)
    ev1: Any = {
        "id": "ev-1",
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "invocation_id": "inv-1",
        "timestamp": ts1,
        "event_data": {"author": "user", "seq": 1},
    }
    ev2: Any = {
        "id": "ev-2",
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "invocation_id": "inv-2",
        "timestamp": ts2,
        "event_data": {"author": "agent", "seq": 2},
    }
    bad_ev: Any = {**ev1, "user_id": "other-user"}
    missing_ev: Any = {**ev1, "session_id": "missing-sess"}

    if not is_async_mode:
        cfg = DuckDBSyncConfig(database=":memory:duck_adk_sess_sync", share_pool=False, extension_config=adk_ext)
        store = DuckDBSyncADKStore(cfg)
        assert not hasattr(store, "__dict__")
        assert store.get_session("app-1", "user-1", "sess-1") is None
        assert store.list_sessions("app-1", "user-1") == []
        assert store.get_events("app-1", "user-1", "sess-1") == []
        assert store.get_app_state("app-1") is None
        assert store.get_user_state("app-1", "user-1") is None
        assert store.get_metadata("schema_ver") is None

        store.create_tables()
        created = store.create_session(
            "sess-1",
            "app-1",
            "user-1",
            {"local": 1},
            app_state_delta={"app:theme": "dark"},
            user_state_delta={"user:lang": "en"},
        )
        assert created["state"] == {"local": 1, "app:theme": "dark", "user:lang": "en"}
        assert store.get_app_state("app-1") == {"app:theme": "dark"}
        assert store.get_user_state("app-1", "user-1") == {"user:lang": "en"}

        second = store.create_session("sess-2", "app-1", "user-1", {"local": 10})
        assert second["state"] == {"local": 10, "app:theme": "dark", "user:lang": "en"}

        updated = store.append_event_and_update_state(
            ev1,
            "app-1",
            "user-1",
            "sess-1",
            {"local": 2},
            app_state_delta={"app:count": 5},
            user_state_delta={"user:tier": "pro"},
        )
        assert updated["state"] == {
            "local": 2,
            "app:theme": "dark",
            "app:count": 5,
            "user:lang": "en",
            "user:tier": "pro",
        }
        store.append_event(ev2)
        assert [e["id"] for e in store.get_events("app-1", "user-1", "sess-1")] == ["ev-1", "ev-2"]
        assert [e["id"] for e in store.get_events("app-1", "user-1", "sess-1", limit=1)] == ["ev-2"]
        assert store.get_events("app-1", "user-1", "sess-1", limit=0) == []

        with pytest.raises(ValueError, match="event_record scope"):
            store.append_event_and_update_state(bad_ev, "app-1", "user-1", "sess-1", {})
        with pytest.raises(ValueError, match="not found"):
            store.append_event(missing_ev)

        renewed = store.get_session("app-1", "user-1", "sess-1", renew_for=60)
        assert renewed is not None
        assert renewed["update_time"] >= updated["update_time"]
        store.set_metadata("schema_ver", "1")
        assert store.get_metadata("schema_ver") == "1"
        store.delete_session("app-1", "other-user", "sess-1")
        assert len(store.get_events("app-1", "user-1", "sess-1")) == 2
        store.delete_session("app-1", "user-1", "sess-1")
        assert store.get_session("app-1", "user-1", "sess-1") is None
        assert store.get_events("app-1", "user-1", "sess-1") == []
        cfg.close()
        return

    acfg = DuckDBAsyncConfig(database=":memory:duck_adk_sess_async", share_pool=False, extension_config=adk_ext)
    astore = DuckDBAsyncADKStore(acfg)
    assert not hasattr(astore, "__dict__")
    assert await astore.get_session("app-1", "user-1", "sess-1") is None
    assert await astore.list_sessions("app-1", "user-1") == []
    assert await astore.get_events("app-1", "user-1", "sess-1") == []
    assert await astore.get_app_state("app-1") is None
    assert await astore.get_user_state("app-1", "user-1") is None
    assert await astore.get_metadata("schema_ver") is None

    await astore.create_tables()
    created_a = await astore.create_session(
        "sess-1",
        "app-1",
        "user-1",
        {"local": 1},
        app_state_delta={"app:theme": "dark"},
        user_state_delta={"user:lang": "en"},
    )
    assert created_a["state"] == {"local": 1, "app:theme": "dark", "user:lang": "en"}
    assert await astore.get_app_state("app-1") == {"app:theme": "dark"}
    assert await astore.get_user_state("app-1", "user-1") == {"user:lang": "en"}

    second_a = await astore.create_session("sess-2", "app-1", "user-1", {"local": 10})
    assert second_a["state"] == {"local": 10, "app:theme": "dark", "user:lang": "en"}

    updated_a = await astore.append_event_and_update_state(
        ev1,
        "app-1",
        "user-1",
        "sess-1",
        {"local": 2},
        app_state_delta={"app:count": 5},
        user_state_delta={"user:tier": "pro"},
    )
    assert updated_a["state"] == {
        "local": 2,
        "app:theme": "dark",
        "app:count": 5,
        "user:lang": "en",
        "user:tier": "pro",
    }
    await astore.append_event(ev2)
    assert [e["id"] for e in await astore.get_events("app-1", "user-1", "sess-1")] == ["ev-1", "ev-2"]
    assert [e["id"] for e in await astore.get_events("app-1", "user-1", "sess-1", limit=1)] == ["ev-2"]
    assert await astore.get_events("app-1", "user-1", "sess-1", limit=0) == []

    with pytest.raises(ValueError, match="event_record scope"):
        await astore.append_event_and_update_state(bad_ev, "app-1", "user-1", "sess-1", {})
    with pytest.raises(ValueError, match="not found"):
        await astore.append_event(missing_ev)

    renewed_a = await astore.get_session("app-1", "user-1", "sess-1", renew_for=60)
    assert renewed_a is not None
    assert renewed_a["update_time"] >= updated_a["update_time"]
    await astore.set_metadata("schema_ver", "1")
    assert await astore.get_metadata("schema_ver") == "1"
    await astore.delete_session("app-1", "other-user", "sess-1")
    assert len(await astore.get_events("app-1", "user-1", "sess-1")) == 2
    await astore.delete_session("app-1", "user-1", "sess-1")
    assert await astore.get_session("app-1", "user-1", "sess-1") is None
    assert await astore.get_events("app-1", "user-1", "sess-1") == []
    await acfg.close()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_duckdb_adk_memory_vector_search_and_owner_scoped_delete(is_async_mode: bool) -> None:
    """Verify DuckDB ADK memory SQL vector similarity ordering, ILIKE fallback, and owner-scoped deletes."""
    adk_cfg: dict[str, Any] = {
        "memory_table": "adk_memory_custom",
        "enable_memory": True,
        "enable_vector": True,
        "vector_dimensions": 2,
        "memory_max_results": 10,
        "use_fts": False,
        "fts_options": {"stemmer": "porter"},
    }
    now = datetime.now(UTC)
    old_ts = now - timedelta(days=30)
    entries: list[Any] = [
        {
            "id": "m1",
            "session_id": "s1",
            "app_name": "app-a",
            "user_id": "u1",
            "scope": "user",
            "event_id": "e1",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "duckdb migration checklist"},
            "content_text": "duckdb migration checklist",
            "metadata_json": {"tag": "db"},
            "inserted_at": now,
            "embedding": [0.1, 0.9],
        },
        {
            "id": "m2",
            "session_id": "s1",
            "app_name": "app-a",
            "user_id": "u2",
            "scope": "user",
            "event_id": "e2",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "duckdb backup policy"},
            "content_text": "duckdb backup policy",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": [0.9, 0.1],
        },
        {
            "id": "m3",
            "session_id": "s1",
            "app_name": "app-b",
            "user_id": "u1",
            "scope": "app",
            "event_id": "e3",
            "author": "agent",
            "timestamp": old_ts,
            "content_json": {"text": "duckdb shared knowledge"},
            "content_text": "duckdb shared knowledge",
            "metadata_json": None,
            "inserted_at": old_ts,
            "embedding": [1.0, 0.0],
        },
        {
            "id": "m4",
            "session_id": "s2",
            "app_name": "app-a",
            "user_id": "u1",
            "scope": "user",
            "event_id": "e4",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "duckdb connection pool"},
            "content_text": "duckdb connection pool",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": [0.95, 0.05],
        },
    ]

    if not is_async_mode:
        cfg = DuckDBSyncConfig(
            database=":memory:duck_adk_mem_sync",
            share_pool=False,
            extension_config={"adk": adk_cfg},
        )
        mem = DuckDBSyncADKMemoryStore(cfg)
        assert not hasattr(mem, "__dict__")
        assert mem.search_entries("duckdb", "app-a", "u1") == []
        mem.create_tables()
        assert mem.insert_memory_entries(entries) == 4
        assert mem.insert_memory_entries([entries[0]]) == 0

        hits = mem.search_entries("duckdb", "app-a", "u1", embedding=[1.0, 0.0])
        assert [h["id"] for h in hits] == ["m4", "m1"]
        assert mem.search_entries("duckdb", "app-a", "u1", scope_filter="app") == []

        deleted = mem.delete_entries_by_session("s1", "app-a", "u1")
        assert deleted == 1
        assert [h["id"] for h in mem.search_entries("duckdb", "app-a", "u1")] == ["m4"]
        assert [h["id"] for h in mem.search_entries("duckdb", "app-a", "u2")] == ["m2"]
        assert [h["id"] for h in mem.search_entries("duckdb", "app-b", "u1")] == ["m3"]
        assert mem.delete_entries_older_than(7, app_name="app-b") == 1
        cfg.close()
        return

    acfg = DuckDBAsyncConfig(
        database=":memory:duck_adk_mem_async",
        share_pool=False,
        extension_config={"adk": adk_cfg},
    )
    amem = DuckDBAsyncADKMemoryStore(acfg)
    assert not hasattr(amem, "__dict__")
    assert await amem.search_entries("duckdb", "app-a", "u1") == []
    await amem.create_tables()
    assert await amem.insert_memory_entries(entries) == 4
    assert await amem.insert_memory_entries([entries[0]]) == 0

    hits_a = await amem.search_entries("duckdb", "app-a", "u1", embedding=[1.0, 0.0])
    assert [h["id"] for h in hits_a] == ["m4", "m1"]
    assert await amem.search_entries("duckdb", "app-a", "u1", scope_filter="app") == []

    deleted_a = await amem.delete_entries_by_session("s1", "app-a", "u1")
    assert deleted_a == 1
    assert [h["id"] for h in await amem.search_entries("duckdb", "app-a", "u1")] == ["m4"]
    assert [h["id"] for h in await amem.search_entries("duckdb", "app-a", "u2")] == ["m2"]
    assert [h["id"] for h in await amem.search_entries("duckdb", "app-b", "u1")] == ["m3"]
    assert await amem.delete_entries_older_than(7, app_name="app-b") == 1
    await acfg.close()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_duckdb_adk_artifact_versioning_and_retention(is_async_mode: bool) -> None:
    """Verify DuckDB ADK artifact versioning, ArtifactReference round-trip, key listing, and deletion."""
    now = datetime.now(UTC)
    old_ts = now - timedelta(days=10)
    v0_record: Any = {
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "filename": "report.json",
        "version": 0,
        "mime_type": "application/json",
        "canonical_uri": "memory://artifacts/report_v0.json",
        "custom_metadata": {"stage": "draft"},
        "created_at": old_ts,
    }
    v1_ref = ArtifactReference(
        uri="memory://artifacts/report_v1.json",
        version=1,
        content_type="application/json",
        size=256,
        updated=now,
        signed_url="https://example.invalid/signed",
    )
    user_record: Any = {
        "app_name": "app-1",
        "user_id": "user-1",
        "session_id": None,
        "filename": "profile.txt",
        "version": 0,
        "mime_type": "text/plain",
        "canonical_uri": "memory://artifacts/profile.txt",
        "custom_metadata": None,
        "created_at": now,
    }

    if not is_async_mode:
        cfg = DuckDBSyncConfig(
            database=":memory:duck_adk_art_sync",
            share_pool=False,
            extension_config={"adk": {"artifact_table": "adk_artifacts_custom"}},
        )
        art = DuckDBSyncADKArtifactStore(cfg)
        assert not hasattr(art, "__dict__")
        assert art.get_artifact("app-1", "user-1", "report.json", session_id="sess-1") is None
        art.create_table()
        assert art.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 0
        art.insert_artifact(v0_record)
        assert art.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 1
        art.insert_artifact_reference(
            "app-1",
            "user-1",
            "report.json",
            v1_ref,
            session_id="sess-1",
            custom_metadata={"stage": "final"},
        )
        art.insert_artifact(user_record)

        latest = art.get_artifact("app-1", "user-1", "report.json", session_id="sess-1")
        assert latest is not None
        assert latest["version"] == 1
        ref = art.get_artifact_reference("app-1", "user-1", "report.json", session_id="sess-1", version=1)
        assert ref is not None
        assert ref.uri == "memory://artifacts/report_v1.json"
        assert ref.size == 256
        assert art.list_artifact_keys("app-1", "user-1") == ["profile.txt"]
        assert art.list_artifact_keys("app-1", "user-1", session_id="sess-1") == ["profile.txt", "report.json"]
        assert [v["version"] for v in art.list_artifact_versions("app-1", "user-1", "report.json", "sess-1")] == [0, 1]

        pruned = art.delete_artifacts_older_than(now - timedelta(days=1), app_name="app-1")
        assert [p["version"] for p in pruned] == [0]
        removed = art.delete_artifact("app-1", "user-1", "report.json", session_id="sess-1")
        assert [r["version"] for r in removed] == [1]
        assert art.get_artifact("app-1", "user-1", "report.json", session_id="sess-1") is None
        cfg.close()
        return

    acfg = DuckDBAsyncConfig(
        database=":memory:duck_adk_art_async",
        share_pool=False,
        extension_config={"adk": {"artifact_table": "adk_artifacts_custom"}},
    )
    aart = DuckDBAsyncADKArtifactStore(acfg)
    assert not hasattr(aart, "__dict__")
    assert await aart.get_artifact("app-1", "user-1", "report.json", session_id="sess-1") is None
    await aart.create_table()
    assert await aart.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 0
    await aart.insert_artifact(v0_record)
    assert await aart.get_next_version("app-1", "user-1", "report.json", session_id="sess-1") == 1
    await aart.insert_artifact_reference(
        "app-1",
        "user-1",
        "report.json",
        v1_ref,
        session_id="sess-1",
        custom_metadata={"stage": "final"},
    )
    await aart.insert_artifact(user_record)

    latest_a = await aart.get_artifact("app-1", "user-1", "report.json", session_id="sess-1")
    assert latest_a is not None
    assert latest_a["version"] == 1
    ref_a = await aart.get_artifact_reference("app-1", "user-1", "report.json", session_id="sess-1", version=1)
    assert ref_a is not None
    assert ref_a.uri == "memory://artifacts/report_v1.json"
    assert ref_a.size == 256
    assert await aart.list_artifact_keys("app-1", "user-1") == ["profile.txt"]
    assert await aart.list_artifact_keys("app-1", "user-1", session_id="sess-1") == ["profile.txt", "report.json"]
    assert [v["version"] for v in await aart.list_artifact_versions("app-1", "user-1", "report.json", "sess-1")] == [
        0,
        1,
    ]

    pruned_a = await aart.delete_artifacts_older_than(now - timedelta(days=1), app_name="app-1")
    assert [p["version"] for p in pruned_a] == [0]
    removed_a = await aart.delete_artifact("app-1", "user-1", "report.json", session_id="sess-1")
    assert [r["version"] for r in removed_a] == [1]
    assert await aart.get_artifact("app-1", "user-1", "report.json", session_id="sess-1") is None
    await acfg.close()


class _RecordingMSSQLReconcileSyncConn(_RecordingSyncConn):
    """Recording sync MSSQL connection serving T-SQL catalog table/index metadata for reconciliation tests."""

    def __init__(self, *, existing_indexes: list[dict[str, Any]] | None = None) -> None:
        super().__init__()
        self.existing_indexes = list(existing_indexes or [])

    def execute_with_description(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[list[Any], list[str]]:
        self.calls.append(("execute_with_description", sql, parameters))
        lowered = sql.lower()
        if "from sys.tables" in lowered or "from information_schema.tables" in lowered:
            return [], ["schema_name", "table_name"]
        if "from sys.indexes" in lowered:
            cols = ["schema_name", "table_name", "index_name", "type_desc", "is_unique", "is_primary"]
            table_param = parameters[-1] if isinstance(parameters, (tuple, list)) and parameters else None
            matched = [row for row in self.existing_indexes if table_param is None or row["table_name"] == table_param]
            return [tuple(row[c] for c in cols) for row in matched], cols
        return [], ["value"]


class _RecordingMSSQLReconcileAsyncConn(_RecordingAsyncConn):
    """Recording async MSSQL connection serving T-SQL catalog table/index metadata for reconciliation tests."""

    def __init__(self, *, existing_indexes: list[dict[str, Any]] | None = None) -> None:
        super().__init__()
        self.existing_indexes = list(existing_indexes or [])

    async def execute_with_description(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[list[Any], list[str]]:
        self.calls.append(("execute_with_description", sql, parameters))
        lowered = sql.lower()
        if "from sys.tables" in lowered or "from information_schema.tables" in lowered:
            return [], ["schema_name", "table_name"]
        if "from sys.indexes" in lowered:
            cols = ["schema_name", "table_name", "index_name", "type_desc", "is_unique", "is_primary"]
            table_param = parameters[-1] if isinstance(parameters, (tuple, list)) and parameters else None
            matched = [row for row in self.existing_indexes if table_param is None or row["table_name"] == table_param]
            return [tuple(row[c] for c in cols) for row in matched], cols
        return [], ["value"]


@pytest.mark.parametrize(
    "store_cls",
    [MSSQLEventQueueStore, MSSQLLitestarStore],
)
def test_mssql_events_litestar_slots_and_reconcile_hooks(store_cls: type[Any]) -> None:
    """Verify MSSQL event/Litestar stores declare `__slots__`, T-SQL hooks, and lazy ADK import isolation."""
    assert "__slots__" in store_cls.__dict__
    cfg = MSSQLSyncConfig(
        url="mssql://sa:pw@127.0.0.1:1433/app",
        extension_config={
            "litestar": {"session_table": "app.vtb_sessions"},
            "events": {"queue_table": "app.vtb_events"},
        },
    )
    store = store_cls(cfg)
    assert not hasattr(store, "__dict__")
    targets = store.get_schema_targets()
    assert len(targets) == 1
    assert targets[0].schema == "app"
    drops = store.downgrade_drops()
    assert any("IF OBJECT_ID" in d.statement and "DROP TABLE" in d.statement for d in drops)

    if store_cls is MSSQLEventQueueStore:
        assert store._index_existence_target() == ("app", "vtb_events")
        companion_stmt = store.companion_index_statement()
        assert companion_stmt is not None
        assert "IF NOT EXISTS (SELECT 1 FROM sys.indexes" in companion_stmt
        assert "CREATE INDEX idx_app_vtb_events_channel_status" in companion_stmt
    else:
        companions = store.get_companion_indexes()
        assert len(companions) == 1
        assert isinstance(companions[0], CompanionIndex)
        assert companions[0].name == "IX_vtb_sessions_expires_at"
        assert companions[0].schema == "app"
        assert companions[0].table == "vtb_sessions"
        assert "IF NOT EXISTS (SELECT 1 FROM sys.indexes" in companions[0].ddl
        assert "WHERE expires_at IS NOT NULL" in companions[0].ddl

    assert mssql_reconcile.down is mssql_reconcile.downgrade
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import vertebra.extensions.sqlspec.adapters.mssql.reconcile; "
                "assert 'google.adk' not in sys.modules; "
                "assert 'vertebra.extensions.sqlspec.adapters.mssql.adk' not in sys.modules"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("is_async_mode", "index_already_present"),
    [
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    ],
)
async def test_mssql_reconcile_companion_index_metadata_and_downgrade(
    is_async_mode: bool,
    index_already_present: bool,
) -> None:
    """Verify MSSQL reconcile checks `get_indexes()` via `metadata_has_index` and emits guarded T-SQL DDL."""
    existing_indexes = (
        [
            {
                "schema_name": "dbo",
                "table_name": "vtb_sessions",
                "index_name": "IX_vtb_sessions_expires_at",
                "type_desc": "NONCLUSTERED",
                "is_unique": 0,
                "is_primary": 0,
            },
            {
                "schema_name": "dbo",
                "table_name": "vtb_events",
                "index_name": "idx_vtb_events_channel_status",
                "type_desc": "NONCLUSTERED",
                "is_unique": 0,
                "is_primary": 0,
            },
        ]
        if index_already_present
        else []
    )
    ext_cfg: dict[str, Any] = {
        "litestar": {"session_table": "vtb_sessions"},
        "events": {"queue_table": "vtb_events"},
    }

    if is_async_mode:
        aconn = _RecordingMSSQLReconcileAsyncConn(existing_indexes=existing_indexes)
        acfg = MSSQLAsyncConfig(
            url="mssql://sa:pw@127.0.0.1:1433/app",
            share_pool=False,
            connection_instance=aconn,
            extension_config=ext_cfg,
        )
        driver = MSSQLAsyncDriver(aconn)
        ctx = MigrationContext(config=acfg, driver=driver)
        awaitable = mssql_reconcile.reconcile(ctx)
        assert awaitable is not None
        await awaitable

        executed = [*aconn.scripts, *(c[1] for c in aconn.calls)]
        all_sql = "\n".join(executed)
        assert "CREATE TABLE [dbo].[vtb_sessions]" in all_sql
        assert "IF OBJECT_ID(N'[dbo].[vtb_events]', N'U') IS NULL BEGIN CREATE TABLE vtb_events" in all_sql
        has_lit_idx = any(
            "CREATE INDEX [IX_vtb_sessions_expires_at]" in s and "sys.indexes" in s and "CREATE TABLE" not in s
            for s in executed
        )
        has_ev_idx = any("CREATE INDEX idx_vtb_events_channel_status" in s and "sys.indexes" in s for s in executed)
        assert has_lit_idx is (not index_already_present)
        assert has_ev_idx is (not index_already_present)

        aconn.scripts.clear()
        aconn.calls.clear()
        down_awaitable = mssql_reconcile.downgrade(ctx)
        assert down_awaitable is not None
        await down_awaitable
        drop_sql = [c[1] for c in aconn.calls if "DROP TABLE" in c[1]]
        assert len(drop_sql) == 2
        assert "[dbo].[vtb_events]" in drop_sql[0]
        assert "[dbo].[vtb_sessions]" in drop_sql[1]
        return

    conn = _RecordingMSSQLReconcileSyncConn(existing_indexes=existing_indexes)
    cfg = MSSQLSyncConfig(
        url="mssql://sa:pw@127.0.0.1:1433/app",
        share_pool=False,
        connection_instance=conn,
        extension_config=ext_cfg,
    )
    sdriver = MSSQLSyncDriver(conn)
    sctx = MigrationContext(config=cfg, driver=sdriver)
    assert mssql_reconcile.reconcile(sctx) is None

    executed = [*conn.scripts, *(c[1] for c in conn.calls)]
    all_sql = "\n".join(executed)
    assert "CREATE TABLE [dbo].[vtb_sessions]" in all_sql
    assert "IF OBJECT_ID(N'[dbo].[vtb_events]', N'U') IS NULL BEGIN CREATE TABLE vtb_events" in all_sql
    has_lit_idx = any(
        "CREATE INDEX [IX_vtb_sessions_expires_at]" in s and "sys.indexes" in s and "CREATE TABLE" not in s
        for s in executed
    )
    has_ev_idx = any("CREATE INDEX idx_vtb_events_channel_status" in s and "sys.indexes" in s for s in executed)
    assert has_lit_idx is (not index_already_present)
    assert has_ev_idx is (not index_already_present)

    conn.scripts.clear()
    conn.calls.clear()
    assert mssql_reconcile.downgrade(sctx) is None
    drop_sql = [c[1] for c in conn.calls if "DROP TABLE" in c[1]]
    assert len(drop_sql) == 2
    assert "[dbo].[vtb_events]" in drop_sql[0]
    assert "[dbo].[vtb_sessions]" in drop_sql[1]


def test_mssql_add_extension_migrations_runner_discovery_and_errors(tmp_path: Path) -> None:
    """Verify `add_extension_migrations` discovers MSSQL reconciler and enforces error contracts."""
    migrations_dir = tmp_path / "mssql_migrations"
    migrations_dir.mkdir()
    cfg = MSSQLSyncConfig(
        url="mssql://sa:pw@127.0.0.1:1433/app",
        migration_config={"script_location": str(migrations_dir)},
    )
    cfg.add_extension_migrations(
        "litestar",
        "vertebra.extensions.sqlspec:adapters/mssql",
        migration_mode="reconcile",
    )
    cfg.add_extension_migrations(
        "events",
        "vertebra.extensions.sqlspec:adapters/mssql",
        migration_mode="reconcile",
    )
    commands = cfg.get_migration_commands()
    assert [desc.name for desc in commands.extension_reconcilers] == ["litestar", "events"]
    assert all(desc.module_path.name == "reconcile.py" for desc in commands.extension_reconcilers)

    driver = MSSQLSyncDriver(_RecordingMSSQLReconcileSyncConn())
    async_cfg = MSSQLAsyncConfig(url="mssql://sa:pw@127.0.0.1:1433/app")
    async_driver = MSSQLAsyncDriver(_RecordingMSSQLReconcileAsyncConn())

    for bad_ctx in (
        MigrationContext(config=None, driver=driver),
        MigrationContext(config=cfg, driver=None),
        MigrationContext(config=cfg, driver=driver, extension_config={"migration_mode": "invalid"}),
        MigrationContext(config=async_cfg, driver=driver),
        MigrationContext(config=cfg, driver=async_driver),
        MigrationContext(config=SQLiteSyncConfig(database=":memory:"), driver=driver),
    ):
        with pytest.raises(MigrationError):
            mssql_reconcile.reconcile(bad_ctx)
        with pytest.raises(MigrationError):
            mssql_reconcile.downgrade(bad_ctx)


@pytest.mark.parametrize(
    "store_cls",
    [DuckDBEventQueueStore, DuckDBLitestarStore],
)
def test_duckdb_events_litestar_slots_and_reconcile_hooks(store_cls: type[Any]) -> None:
    """Verify DuckDB event/Litestar stores declare `__slots__`, hooks, and lazy ADK import isolation."""
    assert "__slots__" in store_cls.__dict__
    cfg = DuckDBSyncConfig(database=":memory:duck_hooks")
    store = store_cls(cfg)
    assert not hasattr(store, "__dict__")
    targets = store.get_schema_targets()
    assert len(targets) == 1
    drops = store.downgrade_drops()
    assert any("DROP TABLE IF EXISTS" in d.statement for d in drops)

    if store_cls is DuckDBLitestarStore:
        companions = store.get_companion_indexes()
        assert len(companions) == 1
        assert isinstance(companions[0], CompanionIndex)
        assert companions[0].name == "idx_litestar_session_expires_at"

    assert duckdb_reconcile.down is duckdb_reconcile.downgrade
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import vertebra.extensions.sqlspec.adapters.duckdb.reconcile; "
                "assert 'google.adk' not in sys.modules; "
                "assert 'vertebra.extensions.sqlspec.adapters.duckdb.adk' not in sys.modules"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True])
async def test_duckdb_reconcile_and_downgrade_live_stores(is_async_mode: bool) -> None:
    """Run live DuckDB `:memory:` reconcile -> companion indexes -> Litestar & Events CRUD -> downgrade."""
    ext_cfg: dict[str, Any] = {
        "litestar": {"session_table": "vtb_sessions"},
        "events": {"queue_table": "vtb_events"},
    }
    if is_async_mode:
        cfg = DuckDBAsyncConfig(database=":memory:duck_reconcile_async", extension_config=ext_cfg)
        try:
            async with cfg.provide_session() as driver:
                ctx = MigrationContext(config=cfg, driver=driver)
                awaitable = duckdb_reconcile.reconcile(ctx)
                assert awaitable is not None
                await awaitable
                await driver.commit()

                tables = {row["table_name"] for row in await driver.data_dictionary.get_tables(driver)}
                assert {"vtb_sessions", "vtb_events"} <= tables

                await driver.execute_script("DROP INDEX idx_vtb_sessions_expires_at;")
                await driver.execute_script("DROP INDEX idx_vtb_events_channel_status;")
                re_awaitable = duckdb_reconcile.reconcile(ctx)
                assert re_awaitable is not None
                await re_awaitable
                await driver.commit()

                lit_idx = await driver.data_dictionary.get_indexes(driver, table="vtb_sessions")
                ev_idx = await driver.data_dictionary.get_indexes(driver, table="vtb_events")
                assert any(row["index_name"] == "idx_vtb_sessions_expires_at" for row in lit_idx)
                assert any(row["index_name"] == "idx_vtb_events_channel_status" for row in ev_idx)

                await driver.execute(
                    "INSERT INTO vtb_events (event_id, channel, payload_json, status) VALUES (?, ?, ?, ?)",
                    ("ev-1", "ch-a", '{"ok":true}', "pending"),
                )
                await driver.commit()
                ev_row = await driver.select_one("SELECT channel, status FROM vtb_events WHERE event_id = ?", ("ev-1",))
                assert ev_row == {"channel": "ch-a", "status": "pending"}

            lit_store = DuckDBLitestarStore(cfg)
            await lit_store.set("sess-a", b"hello", expires_in=timedelta(seconds=60))
            assert await lit_store.exists("sess-a") is True
            assert await lit_store.get("sess-a") == b"hello"
            await lit_store.delete("sess-a")
            assert await lit_store.get("sess-a") is None

            async with cfg.provide_session() as driver:
                down_awaitable = duckdb_reconcile.downgrade(MigrationContext(config=cfg, driver=driver))
                assert down_awaitable is not None
                await down_awaitable
                await driver.commit()
                tables_after = {row["table_name"] for row in await driver.data_dictionary.get_tables(driver)}
                assert "vtb_sessions" not in tables_after
                assert "vtb_events" not in tables_after
        finally:
            await cfg.close()
        return

    sync_cfg = DuckDBSyncConfig(database=":memory:duck_reconcile_sync", extension_config=ext_cfg)
    try:
        with sync_cfg.provide_session() as driver:
            ctx = MigrationContext(config=sync_cfg, driver=driver)
            assert duckdb_reconcile.reconcile(ctx) is None
            driver.commit()

            tables = {row["table_name"] for row in driver.data_dictionary.get_tables(driver)}
            assert {"vtb_sessions", "vtb_events"} <= tables

            driver.execute_script("DROP INDEX idx_vtb_sessions_expires_at;")
            driver.execute_script("DROP INDEX idx_vtb_events_channel_status;")
            assert duckdb_reconcile.reconcile(ctx) is None
            driver.commit()

            lit_idx = driver.data_dictionary.get_indexes(driver, table="vtb_sessions")
            ev_idx = driver.data_dictionary.get_indexes(driver, table="vtb_events")
            assert any(row["index_name"] == "idx_vtb_sessions_expires_at" for row in lit_idx)
            assert any(row["index_name"] == "idx_vtb_events_channel_status" for row in ev_idx)

            driver.execute(
                "INSERT INTO vtb_events (event_id, channel, payload_json, status) VALUES (?, ?, ?, ?)",
                ("ev-1", "ch-a", '{"ok":true}', "pending"),
            )
            driver.commit()
            ev_row = driver.select_one("SELECT channel, status FROM vtb_events WHERE event_id = ?", ("ev-1",))
            assert ev_row == {"channel": "ch-a", "status": "pending"}

        lit_store = DuckDBLitestarStore(sync_cfg)
        await lit_store.set("sess-s", b"sync-val", expires_in=timedelta(seconds=60))
        assert await lit_store.exists("sess-s") is True
        assert await lit_store.get("sess-s") == b"sync-val"
        await lit_store.delete("sess-s")
        assert await lit_store.get("sess-s") is None

        with sync_cfg.provide_session() as driver:
            assert duckdb_reconcile.downgrade(MigrationContext(config=sync_cfg, driver=driver)) is None
            driver.commit()
            tables_after = {row["table_name"] for row in driver.data_dictionary.get_tables(driver)}
            assert "vtb_sessions" not in tables_after
            assert "vtb_events" not in tables_after
    finally:
        sync_cfg.close()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True])
async def test_duckdb_add_extension_migrations_runner_integration(tmp_path: Path, is_async_mode: bool) -> None:
    """Verify `add_extension_migrations` discovers and runs DuckDB reconcile/downgrade via MigrationCommands."""
    migrations_dir = tmp_path / "duckdb_migrations"
    migrations_dir.mkdir()

    if is_async_mode:
        cfg = DuckDBAsyncConfig(
            database=":memory:duck_runner_async",
            migration_config={"script_location": str(migrations_dir)},
        )
        cfg.add_extension_migrations(
            "litestar",
            "vertebra.extensions.sqlspec:adapters/duckdb",
            settings={"session_table": "runner_sessions"},
            migration_mode="reconcile",
        )
        cfg.add_extension_migrations(
            "events",
            "vertebra.extensions.sqlspec:adapters/duckdb",
            settings={"queue_table": "runner_events"},
            migration_mode="reconcile",
        )
        cfg.add_extension_migrations(
            "adk",
            "vertebra.extensions.sqlspec:adapters/duckdb",
            migration_mode="reconcile",
        )
        try:
            commands = cfg.get_migration_commands()
            assert [desc.name for desc in commands.extension_reconcilers] == ["litestar", "events", "adk"]
            upgrade_res = commands.upgrade()
            if upgrade_res is not None:
                await upgrade_res
            async with cfg.provide_session() as driver:
                tables = {row["table_name"] for row in await driver.data_dictionary.get_tables(driver)}
                assert {"runner_sessions", "runner_events", "adk_session", "adk_memory"} <= tables
            downgrade_res = commands.downgrade("base")
            if downgrade_res is not None:
                await downgrade_res
            async with cfg.provide_session() as driver:
                tables_after = {row["table_name"] for row in await driver.data_dictionary.get_tables(driver)}
                assert not ({"runner_sessions", "runner_events", "adk_session", "adk_memory"} & tables_after)
        finally:
            await cfg.close()
        return

    sync_cfg = DuckDBSyncConfig(
        database=":memory:duck_runner_sync",
        migration_config={"script_location": str(migrations_dir)},
    )
    sync_cfg.add_extension_migrations(
        "litestar",
        "vertebra.extensions.sqlspec:adapters/duckdb",
        settings={"session_table": "runner_sessions"},
        migration_mode="reconcile",
    )
    sync_cfg.add_extension_migrations(
        "events",
        "vertebra.extensions.sqlspec:adapters/duckdb",
        settings={"queue_table": "runner_events"},
        migration_mode="reconcile",
    )
    sync_cfg.add_extension_migrations(
        "adk",
        "vertebra.extensions.sqlspec:adapters/duckdb",
        migration_mode="reconcile",
    )
    try:
        commands = sync_cfg.get_migration_commands()
        assert [desc.name for desc in commands.extension_reconcilers] == ["litestar", "events", "adk"]
        commands.upgrade()
        with sync_cfg.provide_session() as driver:
            tables = {row["table_name"] for row in driver.data_dictionary.get_tables(driver)}
            assert {"runner_sessions", "runner_events", "adk_session", "adk_memory"} <= tables
        commands.downgrade("base")
        with sync_cfg.provide_session() as driver:
            tables_after = {row["table_name"] for row in driver.data_dictionary.get_tables(driver)}
            assert not ({"runner_sessions", "runner_events", "adk_session", "adk_memory"} & tables_after)
    finally:
        sync_cfg.close()


@pytest.mark.parametrize(
    "case",
    [
        "missing_config",
        "missing_driver",
        "invalid_mode",
        "sync_async_mismatch",
        "wrong_adapter",
    ],
)
def test_duckdb_reconcile_error_semantics(case: str) -> None:
    """Verify DuckDB reconciler raises `MigrationError` on missing context fields, mode mismatch, or bad mode."""
    sync_cfg = DuckDBSyncConfig(database=":memory:duck_err_sync", extension_config={"litestar": {"session_table": "s"}})
    async_cfg = DuckDBAsyncConfig(
        database=":memory:duck_err_async", extension_config={"litestar": {"session_table": "s"}}
    )
    sync_driver = DuckDBSyncDriver(_RecordingSyncConn())
    async_driver = DuckDBAsyncDriver(_RecordingAsyncConn())

    if case == "missing_config":
        ctx = MigrationContext(config=None, driver=sync_driver)
    elif case == "missing_driver":
        ctx = MigrationContext(config=sync_cfg, driver=None)
    elif case == "invalid_mode":
        ctx = MigrationContext(config=sync_cfg, driver=sync_driver, extension_config={"migration_mode": "bogus"})
    elif case == "sync_async_mismatch":
        ctx = MigrationContext(config=async_cfg, driver=sync_driver)
    else:
        mysql_cfg = MySQLSyncConfig(url="mysql://user:pw@127.0.0.1:3306/app")
        ctx = MigrationContext(config=mysql_cfg, driver=sync_driver)

    with pytest.raises(MigrationError):
        duckdb_reconcile.reconcile(ctx)
    with pytest.raises(MigrationError):
        duckdb_reconcile.downgrade(ctx)

    with pytest.raises(MigrationError):
        duckdb_reconcile.reconcile(MigrationContext(config=sync_cfg, driver=async_driver))


class _DuckDBNestedItem(msgspec.Struct, frozen=True, rename="camel"):
    item_name: str
    item_count: int


class _DuckDBInventoryRow(msgspec.Struct, frozen=True):
    item: _DuckDBNestedItem


def test_sqlspec_0721_nested_msgspec_schema_and_builder_pushdown() -> None:
    """Verify SQLSpec 0.72.1 normalize_nested schema mapping, ASOF join preservation, and subquery scope hashing."""
    cfg = DuckDBSyncConfig(database=":memory:duck_0721_schema")
    try:
        with cfg.provide_session() as session:
            row = session.select_one(
                "SELECT {'item_name': ?, 'item_count': ?} AS item",
                "Pencil",
                2,
            )
            with pytest.raises(msgspec.ValidationError, match="itemName"):
                sqlspec_to_schema(row, schema_type=_DuckDBInventoryRow)
            hydrated = sqlspec_to_schema(row, schema_type=_DuckDBInventoryRow, normalize_nested=True)
            assert hydrated == _DuckDBInventoryRow(
                item=_DuckDBNestedItem(item_name="Pencil", item_count=2),
            )
    finally:
        cfg.close()

    asof_sql = "SELECT u.id, o.price FROM users u ASOF JOIN orders o ON u.ts >= o.ts WHERE o.price > 10"
    builder = sql.select("*", dialect="duckdb")
    builder.set_expression(parse_one(asof_sql, read="duckdb"))
    rendered = " ".join(builder.build().sql.split())
    assert 'ON "u"."ts" >= "o"."ts" WHERE "o"."price" > 10' in rendered

    outer_filter = parse_one("SELECT x FROM (SELECT x + 1 AS x FROM (SELECT 1 AS x)) WHERE x = 1")
    inner_filter = parse_one("SELECT x FROM (SELECT x + 1 AS x FROM (SELECT 1 AS x) WHERE x = 1)")
    assert hash_optimized_expression(outer_filter, "duckdb") != hash_optimized_expression(inner_filter, "duckdb")
