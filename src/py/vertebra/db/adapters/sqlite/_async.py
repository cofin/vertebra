"""Asynchronous SQLite WAL driver backed by `vertebra._sqlite`."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

from vertebra._vertebra import NotFoundError
from vertebra.db.adapters.sqlite._sync import SyncSQLiteDriver, _raise_closed
from vertebra.db.adapters.sqlite.config import SQLiteConfig
from vertebra.db.adapters.sqlite.core import VALID_SQLITE_SYNCHRONOUS
from vertebra.db.adapters.sqlite.pipeline import AsyncSQLitePipeline
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import (
    SQLITE_DIALECT_TAG,
    AsyncDatabaseDriver,
    InterfaceError,
)
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import quote_dialect_identifier, validate_identifier

if TYPE_CHECKING:
    from vertebra._sqlite import AsyncSQLiteClient, AsyncSQLiteCursor

__all__ = ("AsyncSQLiteDriver",)


class AsyncSQLiteDriver(AsyncDatabaseDriver[SQLiteConfig]):
    """Asynchronous SQLite WAL driver backed by native `vertebra._sqlite.AsyncSQLiteClient`."""

    dialect: ClassVar[str] = "sqlite"
    dialect_tag: ClassVar[int] = SQLITE_DIALECT_TAG

    __slots__ = ("_client", "_fetch_dicts", "_fetch_rows", "_fetch_value", "_run_dml")

    _client: AsyncSQLiteClient | None
    _fetch_dicts: Callable[..., Awaitable[list[dict[str, Any]]]]
    _fetch_rows: Callable[..., Awaitable[tuple[list[tuple[Any, ...]], tuple[str, ...]]]]
    _fetch_value: Callable[..., Awaitable[Any]]
    _run_dml: Callable[..., Awaitable[tuple[int, int | None]]]

    def __init__(
        self,
        config: SQLiteConfig | None = None,
        *,
        broker: Any = None,
        _client: AsyncSQLiteClient | None = None,
    ) -> None:
        """Initialize an asynchronous SQLite driver instance."""
        resolved = config or SQLiteConfig()
        if resolved.synchronous not in VALID_SQLITE_SYNCHRONOUS:
            msg = f"Invalid synchronous mode: {resolved.synchronous!r}"
            raise ValueError(msg)
        super().__init__(resolved, broker=broker)
        self._bind_client(_client if _client is not None else self._create_client())

    def _bind_client(self, client: AsyncSQLiteClient | None) -> None:
        """Set the client and bind its single-call methods, or the closed-driver error when there is none."""
        self._client = client
        if client is None:
            self._fetch_dicts = self._fetch_rows = self._fetch_value = self._run_dml = _raise_closed
            return
        self._fetch_dicts = client.fetch_dicts
        self._fetch_rows = client.fetch_rows
        self._fetch_value = client.fetch_value
        self._run_dml = client.execute_dml

    def _create_client(self) -> AsyncSQLiteClient:
        """Create a new native AsyncSQLiteClient handle."""
        native_mod = ensure_module(
            "vertebra._sqlite",
            package="vertebra-core[sqlite]",
            extra="sqlite",
        )
        return cast(
            "AsyncSQLiteClient",
            native_mod.AsyncSQLiteClient(
                path=str(self.config.path),
                wal_mode=bool(self.config.wal_mode),
                synchronous=str(self.config.synchronous),
                mmap_size=int(self.config.mmap_size),
                busy_timeout_ms=int(self.config.busy_timeout_ms),
                max_readers=int(self.config.max_readers),
                foreign_keys=bool(self.config.foreign_keys),
                enable_jsonb=bool(self.config.enable_jsonb),
            ),
        )

    def _require_client(self) -> AsyncSQLiteClient:
        """Return the active native client or raise InterfaceError if closed."""
        if self._client is None or self._closed:
            msg = "SQLite driver is closed"
            raise InterfaceError(msg)
        return self._client

    @property
    def _config(self) -> SQLiteConfig:
        """Return the active SQLiteConfig."""
        return self.config

    @property
    def is_in_memory(self) -> bool:
        """Return whether the target database is in-memory."""
        return self.config.is_in_memory

    @property
    def in_transaction(self) -> bool:
        """Return whether an explicit transaction is currently active."""
        if self._client is not None:
            return bool(self._client.in_transaction)
        return False

    @property
    def lastrowid(self) -> int | None:
        """Return the last captured SQLite INSERT rowid on this driver."""
        client = self._client
        return client.last_statement_rowid if client is not None else None

    @property
    def _sync(self) -> SyncSQLiteDriver:
        """Return a synchronous driver sharing the underlying worker pool."""
        return SyncSQLiteDriver(self.config, _client=self._require_client().to_sync())

    async def connect(self) -> Self:
        """Establish or reopen the native SQLite client connection."""
        if self._client is None or self._closed:
            self._bind_client(self._create_client())
            self._closed = False
        return self

    def cursor(self, *, lazy_begin: bool = False) -> AsyncSQLiteCursor:
        """Return a new native async cursor on this driver's client."""
        return self._require_client().cursor(lazy_begin=lazy_begin)

    async def begin(self) -> None:
        """Begin an explicit SQLite transaction."""
        await self._require_client().begin()

    async def commit(self) -> None:
        """Commit the current SQLite transaction."""
        if self.in_transaction:
            await self._require_client().commit()

    async def rollback(self) -> None:
        """Roll back the current SQLite transaction."""
        if self.in_transaction:
            await self._require_client().rollback()

    async def cancel(self) -> None:
        """Interrupt an active SQLite query on this driver."""
        if self._client is not None and not self._closed:
            self._client.cancel()

    async def close(self) -> None:
        """Close the driver and release client worker connections."""
        self._closed = True
        client = self._client
        self._bind_client(None)
        if client is not None:
            await client.close()

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and hydrate rows into dictionaries or `schema_type` instances."""
        _ = kwargs
        if schema_type is None or schema_type is dict:
            return await self._fetch_dicts(sql, parameters)
        rows, cols = await self._fetch_rows(sql, parameters)
        return self.hydrate_rows(rows, cols, schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        if schema_type is None or schema_type is dict:
            rows = await self._fetch_dicts(sql, parameters)
        else:
            raw, cols = await self._fetch_rows(sql, parameters)
            rows = self.hydrate_rows(raw, cols, schema_type)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or `None` when empty."""
        _ = kwargs
        if schema_type is None or schema_type is dict:
            rows = await self._fetch_dicts(sql, parameters)
        else:
            raw, cols = await self._fetch_rows(sql, parameters)
            rows = self.hydrate_rows(raw, cols, schema_type)
        return rows[0] if rows else None

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        val = await self._fetch_value(sql, parameters)
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return `(tuple_rows, column_names)`."""
        _ = kwargs
        return await self._fetch_rows(sql, parameters)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return unboxed tuple rows."""
        _ = kwargs
        return (await self._fetch_rows(sql, parameters))[0]

    async def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[int, int | None]:
        """Execute a DML or DDL statement and atomically return `(rowcount, lastrowid)`."""
        _ = kwargs
        return await self._run_dml(sql, parameters)

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        _ = kwargs
        return (await self._run_dml(sql, parameters))[0]

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a SQL statement across multiple parameter sets and return total affected rows."""
        _ = kwargs
        if not parameter_sets:
            return 0
        cur = self._require_client().cursor()
        await cur.executemany(sql, parameter_sets)
        return cur.rowcount

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return an ArrowCursor via zero-copy ShmRegion Arrow IPC."""
        _ = (threshold_rows, kwargs)
        native = await self._require_client().select_to_arrow(sql, parameters)
        return native if isinstance(native, ArrowCursor) else ArrowCursor(native)

    async def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """Execute a query asynchronously and yield Arrow record batches."""
        _ = kwargs
        cursor = await self.select_to_arrow(sql, parameters)
        table = cursor.to_arrow_table()
        chunk = max(1, int(batch_size))
        for batch in table.to_batches(max_chunksize=chunk):
            yield batch

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest an Arrow Table, ArrowCursor, or row sequence into table_name."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        if isinstance(data, (bytes, bytearray, memoryview)):
            return await self._require_client().load_from_arrow(table_name, bytes(data))
        if isinstance(data, ArrowCursor):
            return await self._require_client().load_from_arrow(table_name, bytes(data.buffer_view()))
        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        placeholders = ", ".join("?" for _ in cols)
        quoted_table = quote_dialect_identifier(table_name, self.dialect)
        col_list = ", ".join(quote_dialect_identifier(str(col), self.dialect) for col in cols)
        sql = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({placeholders})"
        return await self.execute_many(sql, rows)

    async def execute_script(self, sql_script: str) -> None:
        """Execute multiple SQL statements separated by semicolons."""
        await self._require_client().execute_script(sql_script)

    def pipeline(self) -> AsyncSQLitePipeline:
        """Create a new asynchronous delayed-commit query pipeline."""
        return AsyncSQLitePipeline(self)

    async def execute_pipeline(
        self,
        operations: Sequence[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]],
    ) -> list[Any]:
        """Execute queued pipeline operations asynchronously in a single native worker batch."""
        if not operations:
            return []
        ops = [(kind == "select", sql, params) for kind, sql, params in operations]
        return await self._require_client().execute_pipeline(
            ops,
            use_savepoint=self.in_transaction,
        )

    async def backup_to(
        self,
        target_path: str | os.PathLike[str],
        *,
        pages_per_step: int = 256,
        sleep_ms: int = 0,
    ) -> None:
        """Perform an online backup asynchronously to a target database file."""
        await self._require_client().backup_to(
            str(target_path),
            pages_per_step=pages_per_step,
            sleep_ms=sleep_ms,
        )

    async def restore_from(
        self,
        source_path: str | os.PathLike[str],
        *,
        pages_per_step: int = 256,
        sleep_ms: int = 0,
    ) -> None:
        """Perform an online restore asynchronously from a source database file."""
        await self._require_client().restore_from(
            str(source_path),
            pages_per_step=pages_per_step,
            sleep_ms=sleep_ms,
        )

    async def last_insert_rowid(self) -> int | None:
        """Return the last inserted SQLite rowid asynchronously."""
        raw_id = self._require_client().last_insert_rowid
        return int(raw_id) if raw_id > 0 else None
