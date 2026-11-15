"""SQLAlchemy 2.1 +vertebra / +vtb sync and async dialects, PEP 249 DBAPI bridges, and 3.14t pool adapters."""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects import registry
from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy.dialects.mysql.base import MySQLDialect as BaseMySQLDialect
from sqlalchemy.dialects.oracle.base import OracleDialect as BaseOracleDialect
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.sqlite.base import SQLiteDialect as BaseSQLiteDialect
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.url import URL
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool, Pool, QueuePool, StaticPool

from vertebra.db.dbapi import SyncDBAPIModule
from vertebra.extensions.sqlalchemy.async_adapter import SQLAlchemyAsyncDBAPI
from vertebra.utils.module_loader import import_optional
from vertebra.utils.sync_tools import SyncPool

if TYPE_CHECKING:
    from vertebra.extensions.sqlalchemy.types import (
        DeleteUsingClause,
        InsertSentinelSpec,
    )

__all__ = (
    "ALL_DIALECT_SCHEMES",
    "DIALECT_DESCRIPTORS",
    "DIALECT_SCHEMES",
    "AsyncBigQueryDialect",
    "SyncBigQueryDialect",
    "AsyncDuckDBDialect",
    "SyncDuckDBDialect",
    "AsyncMSSQLDialect",
    "SyncMSSQLDialect",
    "AsyncMySQLDialect",
    "SyncMySQLDialect",
    "NullPool",
    "AsyncOracleDialect",
    "SyncOracleDialect",
    "AsyncPostgreSQLDialect",
    "SyncPostgreSQLDialect",
    "AsyncSQLiteDialect",
    "SyncSQLiteDialect",
    "AsyncSpangresDialect",
    "SyncSpangresDialect",
    "AsyncSpannerDialect",
    "SyncSpannerDialect",
    "SyncEngineAdapter",
    "attach_serverless_disconnect_guard",
    "is_free_threaded",
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

ALL_DIALECT_SCHEMES: tuple[str, ...] = (
    *DIALECT_SCHEMES,
    "postgresql+vertebra_async",
    "postgresql+vtb_async",
    "oracle+vertebra_async",
    "oracle+vtb_async",
    "mysql+vertebra_async",
    "mysql+vtb_async",
    "sqlite+vertebra_async",
    "sqlite+vtb_async",
    "mssql+vertebra",
    "mssql+vtb",
    "mssql+vertebra_async",
    "mssql+vtb_async",
    "duckdb+vertebra",
    "duckdb+vtb",
    "duckdb+vertebra_async",
    "duckdb+vtb_async",
    "bigquery+vertebra",
    "bigquery+vtb",
    "bigquery+vertebra_async",
    "bigquery+vtb_async",
    "spanner+vertebra",
    "spanner+vtb",
    "spanner+vertebra_async",
    "spanner+vtb_async",
    "spangres+vertebra",
    "spangres+vtb",
    "spangres+vertebra_async",
    "spangres+vtb_async",
)


class _NativeTransactionDialect:
    """Forward SQLAlchemy transaction begin to the owning DBAPI connection."""

    __slots__ = ()

    def do_begin(self, dbapi_connection: Any) -> None:
        """Begin through the sync connection or SQLAlchemy's async adaptation."""
        if hasattr(dbapi_connection, "run_async"):
            dbapi_connection.run_async(lambda conn: conn.begin())
        else:
            dbapi_connection.begin()


class SyncPostgreSQLDialect(_NativeTransactionDialect, PGDialect):
    """SQLAlchemy 2.1 synchronous dialect for PostgreSQL (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    default_paramstyle = "numeric_dollar"
    paramstyle = "numeric_dollar"
    use_insertmanyvalues = True
    insert_executemany_returning = True
    supports_native_jsonb = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="numeric_dollar"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise standard QueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return QueuePool

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
        """Adapt Vector or BinaryJSON column type for the PostgreSQL dialect."""
        return col_type


class AsyncPostgreSQLDialect(_NativeTransactionDialect, PGDialect):
    """SQLAlchemy 2.1 asynchronous dialect for PostgreSQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    default_paramstyle = "numeric_dollar"
    paramstyle = "numeric_dollar"

    dbapi: Any = classmethod(lambda cls: SQLAlchemyAsyncDBAPI(paramstyle="numeric_dollar"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise AsyncAdaptedQueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return AsyncAdaptedQueuePool


class SyncSQLiteDialect(_NativeTransactionDialect, BaseSQLiteDialect):
    """SQLAlchemy 2.1 synchronous dialect for SQLite (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return StaticPool for in-memory databases and NullPool when lease brokering is enabled."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        if not url.database or url.database in {":memory:", "memory://", ""}:
            return StaticPool
        return QueuePool


class AsyncSQLiteDialect(_NativeTransactionDialect, BaseSQLiteDialect):
    """SQLAlchemy 2.1 asynchronous dialect for SQLite (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SQLAlchemyAsyncDBAPI(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return StaticPool for in-memory databases and NullPool when lease brokering is enabled."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        if not url.database or url.database in {":memory:", "memory://", ""}:
            return StaticPool
        return AsyncAdaptedQueuePool


class SyncMySQLDialect(_NativeTransactionDialect, BaseMySQLDialect):
    """SQLAlchemy 2.1 synchronous dialect for MySQL (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    paramstyle = "format"
    use_insertmanyvalues = True
    insert_executemany_returning = False
    supports_native_jsonb = False

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="format"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise QueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return QueuePool

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJSON column type for the MySQL dialect."""
        if isinstance(col_type, type):
            return col_type()
        return col_type


class AsyncMySQLDialect(_NativeTransactionDialect, BaseMySQLDialect):
    """SQLAlchemy 2.1 asynchronous dialect for MySQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SQLAlchemyAsyncDBAPI(paramstyle="format"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise AsyncAdaptedQueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return AsyncAdaptedQueuePool


class SyncOracleDialect(_NativeTransactionDialect, BaseOracleDialect):
    """SQLAlchemy 2.1 synchronous dialect for Oracle (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="named"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise QueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return QueuePool

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
        """Adapt Vector or BinaryJSON column type for the Oracle dialect."""
        if isinstance(col_type, type):
            return col_type()
        return col_type


class AsyncOracleDialect(_NativeTransactionDialect, BaseOracleDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Oracle (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SQLAlchemyAsyncDBAPI(paramstyle="named"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise AsyncAdaptedQueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return AsyncAdaptedQueuePool


class SyncMSSQLDialect(_NativeTransactionDialect, MSDialect):
    """SQLAlchemy 2.1 synchronous dialect for Microsoft SQL Server (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise QueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return QueuePool


class AsyncMSSQLDialect(_NativeTransactionDialect, MSDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Microsoft SQL Server (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SQLAlchemyAsyncDBAPI(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise AsyncAdaptedQueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return AsyncAdaptedQueuePool


class SyncDuckDBDialect(_NativeTransactionDialect, DefaultDialect):
    """SQLAlchemy 2.1 synchronous dialect for DuckDB (+vtb / +vertebra)."""

    name = "duckdb"
    driver = "vtb"
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise QueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return QueuePool


class AsyncDuckDBDialect(_NativeTransactionDialect, DefaultDialect):
    """SQLAlchemy 2.1 asynchronous dialect for DuckDB (+vtb_async / +vertebra_async)."""

    name = "duckdb"
    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SQLAlchemyAsyncDBAPI(paramstyle="qmark"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool when lease brokering is enabled, otherwise AsyncAdaptedQueuePool."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        return AsyncAdaptedQueuePool


class SyncBigQueryDialect(_NativeTransactionDialect, DefaultDialect):
    """SQLAlchemy 2.1 synchronous dialect for Google BigQuery (+vtb / +vertebra)."""

    name = "bigquery"
    driver = "vtb"
    paramstyle = "named"
    default_paramstyle = "named"
    supports_statement_cache = True
    supports_native_jsonb = False
    positional = False

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="named"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool so BigQuery delegates connection pooling to DatabaseRegistry."""
        _ = url
        return NullPool

    @classmethod
    def compile_merge_upsert(
        cls,
        target_table: str,
        source_table: str,
        on_keys: Sequence[str],
        update_columns: Sequence[str],
        insert_columns: Sequence[str] | None = None,
    ) -> str:
        """Compile a GoogleSQL `MERGE INTO` upsert statement for BigQuery."""
        if not on_keys:
            msg = "BigQuery MERGE upsert requires at least one key column in on_keys"
            raise ValueError(msg)
        resolved_insert = list(insert_columns) if insert_columns is not None else [*on_keys, *update_columns]
        on_clause = " AND ".join(f"T.{k} = S.{k}" for k in on_keys)
        set_clause = ", ".join(f"T.{c} = S.{c}" for c in update_columns)
        ins_cols = ", ".join(resolved_insert)
        ins_vals = ", ".join(f"S.{c}" for c in resolved_insert)
        return (
            f"MERGE INTO {target_table} AS T "
            f"USING {source_table} AS S "
            f"ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {set_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})"
        )


class AsyncBigQueryDialect(SyncBigQueryDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Google BigQuery (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SQLAlchemyAsyncDBAPI(paramstyle="named"))


class SyncSpannerDialect(_NativeTransactionDialect, DefaultDialect):
    """SQLAlchemy 2.1 synchronous dialect for Cloud Spanner GoogleSQL (+vtb / +vertebra)."""

    name = "spanner"
    driver = "vtb"
    paramstyle = "named"
    default_paramstyle = "named"
    supports_statement_cache = True
    supports_native_jsonb = False
    positional = False

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="named"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool so Cloud Spanner multiplexes over DatabaseRegistry gRPC channels."""
        _ = url
        return NullPool

    @classmethod
    def compile_insert_or_update(
        cls,
        table_name: str,
        columns: Sequence[str],
    ) -> str:
        """Compile a GoogleSQL `INSERT OR UPDATE INTO` statement with `@param` bindings."""
        if not columns:
            msg = "Spanner INSERT OR UPDATE requires at least one column"
            raise ValueError(msg)
        cols_str = ", ".join(columns)
        vals_str = ", ".join(f"@{c}" for c in columns)
        return f"INSERT OR UPDATE INTO {table_name} ({cols_str}) VALUES ({vals_str})"

    @classmethod
    def extract_interleave_options(cls, table_kwargs: Mapping[str, Any]) -> tuple[str | None, str]:
        """Extract `(parent_table, on_delete)` from SQLAlchemy table dialect options."""
        parent = table_kwargs.get("spanner_interleave_in_parent")
        on_delete = str(table_kwargs.get("spanner_interleave_on_delete") or "CASCADE").upper()
        if on_delete not in {"CASCADE", "NO ACTION"}:
            msg = f"Invalid spanner_interleave_on_delete option: {on_delete!r}"
            raise ValueError(msg)
        return (str(parent) if parent else None, on_delete)

    @classmethod
    def compile_interleave_clause(
        cls,
        parent_table: str,
        *,
        on_delete: str = "CASCADE",
    ) -> str:
        """Render a Cloud Spanner `INTERLEAVE IN PARENT` table suffix clause."""
        norm_delete = on_delete.upper()
        if norm_delete not in {"CASCADE", "NO ACTION"}:
            msg = f"Unsupported INTERLEAVE ON DELETE action: {on_delete!r}"
            raise ValueError(msg)
        return f", INTERLEAVE IN PARENT {parent_table} ON DELETE {norm_delete}"


class AsyncSpannerDialect(SyncSpannerDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Cloud Spanner GoogleSQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SQLAlchemyAsyncDBAPI(paramstyle="named"))


class SyncSpangresDialect(_NativeTransactionDialect, PGDialect):
    """SQLAlchemy 2.1 synchronous dialect for Cloud Spanner Spangres (+vtb / +vertebra)."""

    name = "spangres"
    driver = "vtb"
    paramstyle = "pyformat"
    supports_statement_cache = True
    supports_native_jsonb = True

    dbapi: Any = classmethod(lambda cls: SyncDBAPIModule(paramstyle="pyformat"))

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        return cls.dbapi()

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return NullPool so Cloud Spanner Spangres multiplexes over DatabaseRegistry gRPC channels."""
        _ = url
        return NullPool


class AsyncSpangresDialect(SyncSpangresDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Cloud Spanner Spangres (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True

    dbapi: Any = classmethod(lambda cls: SQLAlchemyAsyncDBAPI(paramstyle="pyformat"))


class SyncEngineAdapter:
    """Optional-greenlet SQLAlchemy 2.1 execution adapter for free-threaded Python 3.14t."""

    __slots__ = ("_pool",)

    def __init__(self, *, max_workers: int = 8) -> None:
        """Initialize SyncEngineAdapter with thread pool capacity."""
        self._pool = SyncPool(max_workers=max_workers)

    @property
    def is_free_threaded(self) -> bool:
        """Return True when the interpreter is running with the GIL disabled (3.14t)."""
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


DIALECT_DESCRIPTORS: dict[str, type[DefaultDialect]] = {
    "postgresql+vtb": SyncPostgreSQLDialect,
    "postgresql+vertebra": SyncPostgreSQLDialect,
    "postgresql+vtb_async": AsyncPostgreSQLDialect,
    "postgresql+vertebra_async": AsyncPostgreSQLDialect,
    "mysql+vtb": SyncMySQLDialect,
    "mysql+vertebra": SyncMySQLDialect,
    "mysql+vtb_async": AsyncMySQLDialect,
    "mysql+vertebra_async": AsyncMySQLDialect,
    "sqlite+vtb": SyncSQLiteDialect,
    "sqlite+vertebra": SyncSQLiteDialect,
    "sqlite+vtb_async": AsyncSQLiteDialect,
    "sqlite+vertebra_async": AsyncSQLiteDialect,
    "oracle+vtb": SyncOracleDialect,
    "oracle+vertebra": SyncOracleDialect,
    "oracle+vtb_async": AsyncOracleDialect,
    "oracle+vertebra_async": AsyncOracleDialect,
    "mssql+vtb": SyncMSSQLDialect,
    "mssql+vertebra": SyncMSSQLDialect,
    "mssql+vtb_async": AsyncMSSQLDialect,
    "mssql+vertebra_async": AsyncMSSQLDialect,
    "duckdb+vtb": SyncDuckDBDialect,
    "duckdb+vertebra": SyncDuckDBDialect,
    "duckdb+vtb_async": AsyncDuckDBDialect,
    "duckdb+vertebra_async": AsyncDuckDBDialect,
    "bigquery+vtb": SyncBigQueryDialect,
    "bigquery+vertebra": SyncBigQueryDialect,
    "bigquery+vtb_async": AsyncBigQueryDialect,
    "bigquery+vertebra_async": AsyncBigQueryDialect,
    "spanner+vtb": SyncSpannerDialect,
    "spanner+vertebra": SyncSpannerDialect,
    "spanner+vtb_async": AsyncSpannerDialect,
    "spanner+vertebra_async": AsyncSpannerDialect,
    "spangres+vtb": SyncSpangresDialect,
    "spangres+vertebra": SyncSpangresDialect,
    "spangres+vtb_async": AsyncSpangresDialect,
    "spangres+vertebra_async": AsyncSpangresDialect,
}

_DIALECT_REGISTRATIONS: tuple[tuple[str, str, str], ...] = (
    ("postgresql.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncPostgreSQLDialect"),
    ("postgresql.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncPostgreSQLDialect"),
    ("postgresql.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncPostgreSQLDialect"),
    ("postgresql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncPostgreSQLDialect"),
    ("mysql.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncMySQLDialect"),
    ("mysql.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncMySQLDialect"),
    ("mysql.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncMySQLDialect"),
    ("mysql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncMySQLDialect"),
    ("sqlite.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncSQLiteDialect"),
    ("sqlite.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncSQLiteDialect"),
    ("sqlite.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSQLiteDialect"),
    ("sqlite.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSQLiteDialect"),
    ("oracle.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncOracleDialect"),
    ("oracle.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncOracleDialect"),
    ("oracle.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncOracleDialect"),
    ("oracle.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncOracleDialect"),
    ("mssql.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncMSSQLDialect"),
    ("mssql.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncMSSQLDialect"),
    ("mssql.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncMSSQLDialect"),
    ("mssql.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncMSSQLDialect"),
    ("duckdb.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncDuckDBDialect"),
    ("duckdb.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncDuckDBDialect"),
    ("duckdb.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncDuckDBDialect"),
    ("duckdb.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncDuckDBDialect"),
    ("bigquery.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncBigQueryDialect"),
    ("bigquery.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncBigQueryDialect"),
    ("bigquery.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncBigQueryDialect"),
    ("bigquery.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncBigQueryDialect"),
    ("spanner.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncSpannerDialect"),
    ("spanner.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncSpannerDialect"),
    ("spanner.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSpannerDialect"),
    ("spanner.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSpannerDialect"),
    ("spangres.vtb", "vertebra.extensions.sqlalchemy.dialects", "SyncSpangresDialect"),
    ("spangres.vertebra", "vertebra.extensions.sqlalchemy.dialects", "SyncSpangresDialect"),
    ("spangres.vtb_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSpangresDialect"),
    ("spangres.vertebra_async", "vertebra.extensions.sqlalchemy.dialects", "AsyncSpangresDialect"),
)

for _name, _mod, _cls_name in _DIALECT_REGISTRATIONS:
    registry.register(_name, _mod, _cls_name)
