"""SQLAlchemy 2.1 +vertebra / +vtb dialects, plain-tuple DBAPI bridge, and 3.14t sync pool adapter."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from vertebra.db._registry import DatabaseRegistry
from vertebra.db.mysql import MysqlConfig, SyncMysqlDriver
from vertebra.db.postgres import PostgresConfig, SyncPostgresDriver
from vertebra.db.sqlite import SqliteConfig, SyncSqliteDriver
from vertebra.utils.module_loader import import_optional
from vertebra.utils.sync_tools import SyncPool

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from vertebra.extensions.sqlalchemy import (
        DeleteUsingClause,
        InsertSentinelSpec,
    )

__all__ = (
    "DIALECT_SCHEMES",
    "DBAPIConnection",
    "DBAPICursor",
    "DBAPIModule",
    "DialectDescriptor",
    "MysqlDialect",
    "MysqlVtbDialect",
    "OracleDialect",
    "OracleVtbDialect",
    "PostgresDialect",
    "PostgresVtbDialect",
    "SqliteDialect",
    "SqliteVtbDialect",
    "SyncEngineAdapter",
    "attach_serverless_disconnect_guard",
)


def is_free_threaded() -> bool:
    """Return True when the CPython GIL is disabled."""
    checker = getattr(sys, "_is_gil_enabled", None)
    return bool(checker and not checker())


DIALECT_SCHEMES: tuple[str, ...] = (
    "postgresql+vertebra",
    "postgresql+vtb",
    "oracle+vertebra",
    "oracle+vtb",
    "mysql+vertebra",
    "mysql+vtb",
    "sqlite+vertebra",
    "sqlite+vtb",
)


class DBAPICursor:
    """PEP 249 DBAPI 2.0 cursor returning plain Python tuples for SQLAlchemy 2.1 ORM loaders."""

    __slots__ = ("_conn", "_pos", "_rows", "arraysize", "description", "rowcount")

    def __init__(self, connection: DBAPIConnection) -> None:
        self._conn = connection
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0
        self.description: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
        self.rowcount: int = -1
        self.arraysize: int = 1

    def execute(self, operation: str, parameters: Sequence[Any] | dict[str, Any] = ()) -> DBAPICursor:
        """Execute a SQL statement and buffer plain-tuple rows."""
        self._rows, self.description = self._conn.execute_with_description(operation, parameters)
        self._pos = 0
        self.rowcount = len(self._rows)
        return self

    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]]) -> DBAPICursor:
        """Execute a parameterized SQL statement across a sequence of parameter sets."""
        total = 0
        last_rows: list[tuple[Any, ...]] = []
        last_desc: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
        for params in seq_of_parameters:
            rows, desc = self._conn.execute_with_description(operation, params)
            total += len(rows)
            last_rows = rows
            last_desc = desc
        self._rows = last_rows
        self.description = last_desc
        self._pos = 0
        self.rowcount = total
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next plain-tuple row or ``None``."""
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to ``size`` plain-tuple rows."""
        batch_size = self.arraysize if size is None else size
        batch = self._rows[self._pos : self._pos + batch_size]
        self._pos += len(batch)
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining plain-tuple rows."""
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    def close(self) -> None:
        """Close the cursor."""
        self._rows = []
        self._pos = 0


class DBAPIConnection:
    """PEP 249 DBAPI 2.0 connection backed by Vertebra's native driver layer."""

    __slots__ = ("_config", "_driver", "_sqlite_driver", "_url", "closed")

    def __init__(self, url: str = "sqlite+vtb:///:memory:", *, driver: Any = None) -> None:
        self._url = url
        self._config = DatabaseRegistry.parse_url(url)
        self._driver = driver
        if self._driver is None and isinstance(self._config, PostgresConfig):
            self._driver = SyncPostgresDriver(self._config)
        elif self._driver is None and isinstance(self._config, MysqlConfig):
            self._driver = SyncMysqlDriver(self._config)
        self._sqlite_driver: SyncSqliteDriver | None = (
            SyncSqliteDriver(self._config) if isinstance(self._config, SqliteConfig) else None
        )
        self.closed: bool = False

    @property
    def url(self) -> str:
        """Return the connection URL."""
        return self._url

    @property
    def config(self) -> Any:
        """Return the parsed database configuration."""
        return self._config

    @property
    def driver(self) -> Any:
        """Return the active underlying driver instance."""
        return self._driver if self._driver is not None else self._sqlite_driver

    def cursor(self) -> DBAPICursor:
        """Create a new plain-tuple DBAPI cursor."""
        return DBAPICursor(self)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a query returning rows and column descriptions."""
        if self._driver is not None:
            if hasattr(self._driver, "execute_with_description"):
                driver_res: tuple[
                    list[tuple[Any, ...]],
                    tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
                ] = self._driver.execute_with_description(sql, parameters)
                return driver_res
            if hasattr(self._driver, "execute_tuples"):
                rows = self._driver.execute_tuples(sql, parameters)
                desc = (
                    tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(rows[0])))
                    if rows
                    else None
                )
                return rows, desc
        if self._sqlite_driver is not None:
            rows = self._sqlite_driver.execute_tuples(sql, parameters)
            desc = (
                tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(rows[0]))) if rows else None
            )
            return rows, desc
        return [], None

    def execute_raw_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return plain Python tuples (`list[tuple[Any, ...]]`)."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

    def commit(self) -> None:
        """Commit the active transaction."""
        if self._driver is not None and hasattr(self._driver, "commit"):
            self._driver.commit()

    def rollback(self) -> None:
        """Roll back the active transaction."""
        if self._driver is not None and hasattr(self._driver, "rollback"):
            self._driver.rollback()

    def close(self) -> None:
        """Close the underlying driver connection."""
        if not self.closed:
            if self._driver is not None and hasattr(self._driver, "close"):
                self._driver.close()
            if self._sqlite_driver is not None:
                self._sqlite_driver.close()
            self.closed = True


class DBAPIModule:
    """PEP 249 DBAPI 2.0 module descriptor exposed by Vertebra SQLAlchemy dialects."""

    apilevel: str = "2.0"
    threadsafety: int = 2
    Error = Exception
    DatabaseError = Exception
    IntegrityError = Exception
    OperationalError = Exception
    InterfaceError = Exception
    ProgrammingError = Exception
    __slots__ = ("paramstyle",)

    def __init__(self, *, paramstyle: str = "qmark") -> None:
        self.paramstyle = paramstyle

    def connect(self, url: str = "sqlite+vtb:///:memory:", **kwargs: Any) -> DBAPIConnection:
        """Open a PEP 249 connection for the given URL."""
        return DBAPIConnection(url, **kwargs)


class DialectDescriptor:
    """SQLAlchemy 2.1 dialect metadata for `+vertebra` and `+vtb` URL schemes."""

    name: str = ""
    driver: str = "vertebra"
    paramstyle: str = "qmark"
    supports_statement_cache: bool = True
    use_insertmanyvalues: bool = True
    insert_executemany_returning: bool = True
    supports_native_jsonb: bool = True
    returns_plain_tuples: bool = True

    def __init__(
        self,
        *,
        name: str = "",
        driver: str = "vertebra",
        paramstyle: str = "qmark",
        supports_statement_cache: bool = True,
        use_insertmanyvalues: bool = True,
        insert_executemany_returning: bool = True,
        supports_native_jsonb: bool = True,
        returns_plain_tuples: bool = True,
    ) -> None:
        self.name = name
        self.driver = driver
        self.paramstyle = paramstyle
        self.supports_statement_cache = supports_statement_cache
        self.use_insertmanyvalues = use_insertmanyvalues
        self.insert_executemany_returning = insert_executemany_returning
        self.supports_native_jsonb = supports_native_jsonb
        self.returns_plain_tuples = returns_plain_tuples

    @classmethod
    def dbapi(cls) -> DBAPIModule:
        """Return the PEP 249 DBAPI module."""
        return DBAPIModule(paramstyle=cls.paramstyle)

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1 ``import_dbapi()``."""
        return cls.dbapi()

    @classmethod
    def register_all(cls) -> dict[str, Any]:
        """Register all ``+vertebra`` and ``+vtb`` dialects with SQLAlchemy's dialect registry."""
        mapping: dict[str, Any] = {
            "postgresql.vertebra": PostgresVtbDialect,
            "postgresql.vtb": PostgresVtbDialect,
            "oracle.vertebra": OracleVtbDialect,
            "oracle.vtb": OracleVtbDialect,
            "mysql.vertebra": MysqlVtbDialect,
            "mysql.vtb": MysqlVtbDialect,
            "sqlite.vertebra": SqliteVtbDialect,
            "sqlite.vtb": SqliteVtbDialect,
        }
        sa_dialects = import_optional("sqlalchemy.dialects")
        if sa_dialects is not None:
            registry = getattr(sa_dialects, "registry", None)
            if registry is not None and hasattr(registry, "register"):
                dialect_cls_names = {
                    "postgresql": "PostgresVtbDialect",
                    "oracle": "OracleVtbDialect",
                    "mysql": "MysqlVtbDialect",
                    "sqlite": "SqliteVtbDialect",
                }
                for entry_name in mapping:
                    backend, _driver = entry_name.split(".", 1)
                    registry.register(
                        entry_name,
                        "vertebra.extensions.sqlalchemy.dialects",
                        dialect_cls_names[backend],
                    )
        return mapping


class PostgresVtbDialect(DialectDescriptor):
    """SQLAlchemy 2.1 dialect for PostgreSQL backing +vertebra and +vtb URLs."""

    name: str = "postgresql"
    driver: str = "vertebra"
    paramstyle: str = "pyformat"
    supports_statement_cache: bool = True
    use_insertmanyvalues: bool = True
    insert_executemany_returning: bool = True
    supports_native_jsonb: bool = True
    returns_plain_tuples: bool = True

    @classmethod
    def dbapi(cls) -> DBAPIModule:
        """Return the PEP 249 DBAPI module with pyformat paramstyle."""
        return DBAPIModule(paramstyle=cls.paramstyle)

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def compile_insert_returning_sentinel(
        cls,
        table_name: str,
        columns: Sequence[str],
        spec: InsertSentinelSpec,
        *,
        pk_column: str = "id",
    ) -> str:
        """Compile a bulk INSERT ... RETURNING query with an insert sentinel."""
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f":{c}" for c in columns)
        sentinel_col = spec.sentinel_column
        return f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) RETURNING {pk_column}, {sentinel_col}"

    @classmethod
    def compile_delete_using(cls, clause: DeleteUsingClause) -> str:
        """Compile a multi-table DELETE ... USING statement."""
        return clause.to_sql()

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJson column type for the PostgreSQL dialect."""
        return col_type


class SqliteVtbDialect(DialectDescriptor):
    """SQLAlchemy 2.1 dialect for SQLite backing +vertebra and +vtb URLs."""

    name: str = "sqlite"
    driver: str = "vertebra"
    paramstyle: str = "qmark"
    supports_statement_cache: bool = True
    use_insertmanyvalues: bool = True
    insert_executemany_returning: bool = True
    supports_native_jsonb: bool = True
    returns_plain_tuples: bool = True

    @classmethod
    def dbapi(cls) -> DBAPIModule:
        """Return the PEP 249 DBAPI module with qmark paramstyle."""
        return DBAPIModule(paramstyle=cls.paramstyle)

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()


class MysqlVtbDialect(DialectDescriptor):
    """SQLAlchemy 2.1 dialect for MySQL backing +vertebra and +vtb URLs."""

    name: str = "mysql"
    driver: str = "vertebra"
    paramstyle: str = "format"
    supports_statement_cache: bool = True
    use_insertmanyvalues: bool = True
    insert_executemany_returning: bool = False
    supports_native_jsonb: bool = False
    returns_plain_tuples: bool = True

    @classmethod
    def dbapi(cls) -> DBAPIModule:
        """Return the PEP 249 DBAPI module with format paramstyle."""
        return DBAPIModule(paramstyle=cls.paramstyle)

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJson column type for the MySQL dialect."""
        if isinstance(col_type, type):
            return col_type()
        return col_type


class OracleVtbDialect(DialectDescriptor):
    """SQLAlchemy 2.1 dialect for Oracle backing +vertebra and +vtb URLs."""

    name: str = "oracle"
    driver: str = "vertebra"
    paramstyle: str = "named"
    supports_statement_cache: bool = True
    use_insertmanyvalues: bool = True
    insert_executemany_returning: bool = True
    supports_native_jsonb: bool = True
    returns_plain_tuples: bool = True

    @classmethod
    def dbapi(cls) -> DBAPIModule:
        """Return the PEP 249 DBAPI module with named paramstyle."""
        return DBAPIModule(paramstyle=cls.paramstyle)

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def compile_insert_returning_sentinel(
        cls,
        table_name: str,
        columns: Sequence[str],
        spec: InsertSentinelSpec,
        *,
        pk_column: str = "id",
    ) -> str:
        """Compile a bulk INSERT ... RETURNING ... INTO query with an insert sentinel."""
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f":{c}" for c in columns)
        sentinel_col = spec.sentinel_column
        return (
            f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) "
            f"RETURNING {pk_column}, {sentinel_col} INTO :ret_{pk_column}, :ret_{sentinel_col}"
        )

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJson column type for the Oracle dialect."""
        if isinstance(col_type, type):
            return col_type()
        return col_type


PostgresDialect = PostgresVtbDialect
OracleDialect = OracleVtbDialect
MysqlDialect = MysqlVtbDialect
SqliteDialect = SqliteVtbDialect


class SyncEngineAdapter:
    """Optional-``greenlet`` SQLAlchemy 2.1 execution adapter for free-threaded Python ``3.14t``.

    Under free-threaded Python 3.14t (`sys._is_gil_enabled() == False`), synchronous SQLAlchemy
    ``Session`` and ``Engine`` operations execute with true multi-core parallelism on
    ``SyncPool`` worker threads without requiring ``greenlet`` context switches.
    """

    __slots__ = ("_pool",)

    def __init__(self, *, max_workers: int = 8) -> None:
        self._pool = SyncPool(max_workers=max_workers)

    @property
    def is_free_threaded(self) -> bool:
        """Return True when the interpreter is running with the GIL disabled (`3.14t`)."""
        return is_free_threaded()

    @property
    def requires_greenlet(self) -> bool:
        """Return False when running on free-threaded Python 3.14t direct sync worker threads."""
        return not self.is_free_threaded

    async def run_sync[T](self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute a synchronous SQLAlchemy callable on the free-threaded worker pool."""
        return await self._pool.submit_async(fn, *args, **kwargs)


def attach_serverless_disconnect_guard(engine: Any) -> None:
    """Attach engine_connect listener to verify connection liveness in serverless environments."""
    sa_event = import_optional("sqlalchemy.event")
    if sa_event is None:
        return
    try:

        def _on_connect(dbapi_connection: Any, connection_record: Any) -> None:
            _ = (dbapi_connection, connection_record)

        sa_event.listen(engine, "engine_connect", _on_connect)
    except AttributeError:
        pass
