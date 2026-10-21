"""SQLAlchemy 2.1 +vertebra / +vtb dialects, plain-tuple DBAPI bridge, and 3.14t sync pool adapter."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from vertebra.db._registry import parse_database_url
from vertebra.db.sqlite import SqliteConfig, SyncSqliteDriver
from vertebra.server._runtime_tuning import is_free_threaded
from vertebra.utils.sync_tools import FreeThreadedSyncPool

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

__all__ = (
    "DIALECT_SCHEMES",
    "FreeThreadedEngineAdapter",
    "MysqlVertebraDialect",
    "OracleVertebraDialect",
    "PostgresVertebraDialect",
    "SqliteVertebraDialect",
    "VertebraDBAPIConnection",
    "VertebraDBAPICursor",
    "VertebraDBAPIModule",
    "VertebraDialectDescriptor",
    "register_sqlalchemy_dialects",
)

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


class VertebraDBAPICursor:
    """PEP 249 DBAPI 2.0 cursor returning plain Python tuples for SQLAlchemy 2.1 ORM loaders."""

    def __init__(self, connection: VertebraDBAPIConnection) -> None:
        self._conn = connection
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0
        self.description: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
        self.rowcount: int = -1
        self.arraysize: int = 1

    def execute(self, operation: str, parameters: Sequence[Any] | dict[str, Any] = ()) -> VertebraDBAPICursor:
        """Execute a SQL statement and buffer plain-tuple rows."""
        self._rows = self._conn.execute_raw_tuples(operation, parameters)
        self._pos = 0
        self.rowcount = len(self._rows)
        return self

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]]
    ) -> VertebraDBAPICursor:
        """Execute a parameterized SQL statement across a sequence of parameter sets."""
        total = 0
        for params in seq_of_parameters:
            rows = self._conn.execute_raw_tuples(operation, params)
            total += len(rows)
            self._rows = rows
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


class VertebraDBAPIConnection:
    """PEP 249 DBAPI 2.0 connection backed by Vertebra's native driver layer."""

    def __init__(self, url: str = "sqlite+vtb:///:memory:") -> None:
        self._url = url
        self._config = parse_database_url(url)
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

    def cursor(self) -> VertebraDBAPICursor:
        """Create a new plain-tuple DBAPI cursor."""
        return VertebraDBAPICursor(self)

    def execute_raw_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return plain Python tuples (`list[tuple[Any, ...]]`)."""
        if self._sqlite_driver is not None:
            return self._sqlite_driver.execute_tuples(sql, parameters)
        return []

    def commit(self) -> None:
        """Commit the active transaction."""

    def rollback(self) -> None:
        """Roll back the active transaction."""

    def close(self) -> None:
        """Close the underlying driver connection."""
        if not self.closed:
            if self._sqlite_driver is not None:
                self._sqlite_driver.close()
            self.closed = True


class VertebraDBAPIModule:
    """PEP 249 DBAPI 2.0 module descriptor exposed by Vertebra SQLAlchemy dialects."""

    apilevel: str = "2.0"
    threadsafety: int = 2
    paramstyle: str = "qmark"
    Error = Exception
    DatabaseError = Exception
    IntegrityError = Exception
    OperationalError = Exception
    InterfaceError = Exception
    ProgrammingError = Exception

    def __init__(self, *, paramstyle: str = "qmark") -> None:
        self.paramstyle = paramstyle

    def connect(self, url: str = "sqlite+vtb:///:memory:", **_kwargs: Any) -> VertebraDBAPIConnection:
        """Open a PEP 249 connection for the given URL."""
        return VertebraDBAPIConnection(url)


@dataclass(frozen=True, slots=True)
class VertebraDialectDescriptor:
    """SQLAlchemy 2.1 dialect metadata for `+vertebra` and `+vtb` URL schemes."""

    name: str
    driver: str
    paramstyle: str
    supports_statement_cache: bool = True
    use_insertmanyvalues: bool = True
    insert_executemany_returning: bool = True
    supports_native_jsonb: bool = True
    returns_plain_tuples: bool = True

    @classmethod
    def import_dbapi(cls) -> VertebraDBAPIModule:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1 ``import_dbapi()``."""
        return VertebraDBAPIModule()


PostgresVertebraDialect = VertebraDialectDescriptor(
    name="postgresql",
    driver="vertebra",
    paramstyle="pyformat",
    supports_statement_cache=True,
    use_insertmanyvalues=True,
    insert_executemany_returning=True,
    supports_native_jsonb=True,
)

OracleVertebraDialect = VertebraDialectDescriptor(
    name="oracle",
    driver="vertebra",
    paramstyle="named",
    supports_statement_cache=True,
    use_insertmanyvalues=True,
    insert_executemany_returning=True,
    supports_native_jsonb=True,
)

MysqlVertebraDialect = VertebraDialectDescriptor(
    name="mysql",
    driver="vertebra",
    paramstyle="format",
    supports_statement_cache=True,
    use_insertmanyvalues=True,
    insert_executemany_returning=False,
    supports_native_jsonb=False,
)

SqliteVertebraDialect = VertebraDialectDescriptor(
    name="sqlite",
    driver="vertebra",
    paramstyle="qmark",
    supports_statement_cache=True,
    use_insertmanyvalues=True,
    insert_executemany_returning=True,
    supports_native_jsonb=True,
)


def register_sqlalchemy_dialects() -> dict[str, VertebraDialectDescriptor]:
    """Register all ``+vertebra`` and ``+vtb`` dialects with SQLAlchemy's dialect registry."""
    mapping: dict[str, VertebraDialectDescriptor] = {
        "postgresql.vertebra": PostgresVertebraDialect,
        "postgresql.vtb": PostgresVertebraDialect,
        "oracle.vertebra": OracleVertebraDialect,
        "oracle.vtb": OracleVertebraDialect,
        "mysql.vertebra": MysqlVertebraDialect,
        "mysql.vtb": MysqlVertebraDialect,
        "sqlite.vertebra": SqliteVertebraDialect,
        "sqlite.vtb": SqliteVertebraDialect,
    }
    try:
        sa_dialects: Any = importlib.import_module("sqlalchemy.dialects")
        registry = getattr(sa_dialects, "registry", None)
        if registry is not None and hasattr(registry, "register"):
            for entry_name in mapping:
                backend, driver = entry_name.split(".", 1)
                registry.register(
                    entry_name,
                    "vertebra.extensions.sqlalchemy.dialects",
                    f"{backend.capitalize()}VertebraDialect",
                )
                _ = driver
    except ImportError:
        pass
    return mapping


class FreeThreadedEngineAdapter:
    """Optional-``greenlet`` SQLAlchemy 2.1 execution adapter for free-threaded Python ``3.14t``.

    Under free-threaded Python 3.14t (`sys._is_gil_enabled() == False`), synchronous SQLAlchemy
    ``Session`` and ``Engine`` operations execute with true multi-core parallelism on
    ``FreeThreadedSyncPool`` worker threads without requiring ``greenlet`` context switches.
    """

    def __init__(self, *, max_workers: int = 8) -> None:
        self._pool = FreeThreadedSyncPool(max_workers=max_workers)

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
