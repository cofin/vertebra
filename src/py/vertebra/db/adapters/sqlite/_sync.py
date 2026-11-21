"""Synchronous SQLite WAL driver backed by `vertebra._sqlite`."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, NoReturn, Self, cast

from vertebra._vertebra import NotFoundError
from vertebra.db.adapters.sqlite.config import SQLiteConfig
from vertebra.db.adapters.sqlite.core import VALID_SQLITE_SYNCHRONOUS
from vertebra.db.adapters.sqlite.pipeline import SyncSQLitePipeline
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import (
    SQLITE_DIALECT_TAG,
    InterfaceError,
    SyncDatabaseDriver,
)
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import quote_dialect_identifier, validate_identifier

if TYPE_CHECKING:
    from vertebra._sqlite import SyncSQLiteClient, SyncSQLiteCursor

__all__ = ("SyncSQLiteDriver",)


def _raise_closed(*args: Any, **kwargs: Any) -> NoReturn:
    """Raise the closed-driver error in place of a native call."""
    _ = (args, kwargs)
    msg = "SQLite driver is closed"
    raise InterfaceError(msg)


class SyncSQLiteDriver(SyncDatabaseDriver[SQLiteConfig]):
    """Synchronous SQLite WAL driver backed by native `vertebra._sqlite.SyncSQLiteClient`."""

    dialect: ClassVar[str] = "sqlite"
    dialect_tag: ClassVar[int] = SQLITE_DIALECT_TAG

    __slots__ = ("_client", "_fetch_dicts", "_fetch_rows", "_fetch_value", "_run_dml", "cursor")

    _client: SyncSQLiteClient | None
    _fetch_dicts: Callable[..., list[dict[str, Any]]]
    _fetch_rows: Callable[..., tuple[list[tuple[Any, ...]], tuple[str, ...]]]
    _fetch_value: Callable[..., Any]
    _run_dml: Callable[..., tuple[int, int | None]]

    if TYPE_CHECKING:

        def cursor(self, *, lazy_begin: bool = False) -> SyncSQLiteCursor:
            """Return a native synchronous SQLite cursor bound to this driver."""
            ...

    def __init__(
        self,
        config: SQLiteConfig | None = None,
        *,
        broker: Any = None,
        _client: SyncSQLiteClient | None = None,
    ) -> None:
        """Initialize a synchronous SQLite driver instance."""
        resolved = config or SQLiteConfig()
        if resolved.synchronous not in VALID_SQLITE_SYNCHRONOUS:
            msg = f"Invalid synchronous mode: {resolved.synchronous!r}"
            raise ValueError(msg)
        super().__init__(resolved, broker=broker)
        self._bind_client(_client if _client is not None else self._create_client())

    def _bind_client(self, client: SyncSQLiteClient | None) -> None:
        """Set the client and bind its native methods, or the closed-driver error when there is none."""
        self._client = client
        if client is None:
            self._fetch_dicts = self._fetch_rows = self._fetch_value = self._run_dml = _raise_closed
            object.__setattr__(self, "cursor", _raise_closed)
            return
        self._fetch_dicts = client.fetch_dicts
        self._fetch_rows = client.fetch_rows
        self._fetch_value = client.fetch_value
        self._run_dml = client.execute_dml
        object.__setattr__(self, "cursor", client.cursor)

    def _create_client(self) -> SyncSQLiteClient:
        """Create a new native SyncSQLiteClient handle."""
        native_mod = ensure_module(
            "vertebra._sqlite",
            package="vertebra-core[sqlite]",
            extra="sqlite",
        )
        return cast(
            "SyncSQLiteClient",
            native_mod.SyncSQLiteClient(
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

    def _require_client(self) -> SyncSQLiteClient:
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

    def connect(self) -> Self:
        """Establish or reopen the native SQLite client connection."""
        if self._client is None or self._closed:
            self._bind_client(self._create_client())
            self._closed = False
        return self

    def begin(self) -> None:
        """Begin an explicit SQLite transaction."""
        self._require_client().begin()

    def commit(self) -> None:
        """Commit the current SQLite transaction."""
        if self.in_transaction:
            self._require_client().commit()

    def rollback(self) -> None:
        """Roll back the current SQLite transaction."""
        if self.in_transaction:
            self._require_client().rollback()

    def cancel(self) -> None:
        """Interrupt an active SQLite query on this driver."""
        if self._client is not None and not self._closed:
            self._client.cancel()

    def close(self) -> None:
        """Close the driver and release client worker connections."""
        self._closed = True
        client = self._client
        self._bind_client(None)
        if client is not None:
            client.close()

    def select(
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
            return self._fetch_dicts(sql, parameters)
        rows, cols = self._fetch_rows(sql, parameters)
        return self.hydrate_rows(rows, cols, schema_type)

    def select_one(
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
            rows = self._fetch_dicts(sql, parameters)
        else:
            raw, cols = self._fetch_rows(sql, parameters)
            rows = self.hydrate_rows(raw, cols, schema_type)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    def select_one_or_none(
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
            rows = self._fetch_dicts(sql, parameters)
        else:
            raw, cols = self._fetch_rows(sql, parameters)
            rows = self.hydrate_rows(raw, cols, schema_type)
        return rows[0] if rows else None

    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        val = self._fetch_value(sql, parameters)
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return `(tuple_rows, column_names)`."""
        _ = kwargs
        return self._fetch_rows(sql, parameters)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return unboxed tuple rows."""
        _ = kwargs
        return self._fetch_rows(sql, parameters)[0]

    def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[int, int | None]:
        """Execute a DML or DDL statement and atomically return `(rowcount, lastrowid)`."""
        _ = kwargs
        return self._run_dml(sql, parameters)

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        _ = kwargs
        return self._run_dml(sql, parameters)[0]

    def execute_many(
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
        cur.executemany(sql, parameter_sets)
        return cur.rowcount

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query and return an ArrowCursor via zero-copy ShmRegion Arrow IPC."""
        _ = (threshold_rows, kwargs)
        native = self._require_client().select_to_arrow(sql, parameters)
        return native if isinstance(native, ArrowCursor) else ArrowCursor(native)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """Execute a query and yield Arrow record batches."""
        _ = kwargs
        cursor = self.select_to_arrow(sql, parameters)
        table = cursor.to_arrow_table()
        chunk = max(1, int(batch_size))
        yield from table.to_batches(max_chunksize=chunk)

    def load_from_arrow(
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
            return self._require_client().load_from_arrow(table_name, bytes(data))
        if isinstance(data, ArrowCursor):
            return self._require_client().load_from_arrow(table_name, bytes(data.buffer_view()))
        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        placeholders = ", ".join("?" for _ in cols)
        quoted_table = quote_dialect_identifier(table_name, self.dialect)
        col_list = ", ".join(quote_dialect_identifier(str(col), self.dialect) for col in cols)
        sql = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({placeholders})"
        return self.execute_many(sql, rows)

    def execute_script(self, sql_script: str) -> None:
        """Execute multiple SQL statements separated by semicolons."""
        self._require_client().execute_script(sql_script)

    def pipeline(self) -> SyncSQLitePipeline:
        """Create a new synchronous delayed-commit query pipeline."""
        return SyncSQLitePipeline(self)

    def execute_pipeline(
        self,
        operations: Sequence[tuple[str, str, Sequence[Any] | Mapping[str, Any] | None]],
    ) -> list[Any]:
        """Execute queued pipeline operations atomically in a single native worker batch."""
        if not operations:
            return []
        ops = [(kind == "select", sql, params) for kind, sql, params in operations]
        return self._require_client().execute_pipeline(
            ops,
            use_savepoint=self.in_transaction,
        )

    def backup_to(
        self,
        target_path: str | os.PathLike[str],
        *,
        pages_per_step: int = 256,
        sleep_ms: int = 0,
    ) -> None:
        """Perform an online backup to a target database file."""
        self._require_client().backup_to(
            str(target_path),
            pages_per_step=pages_per_step,
            sleep_ms=sleep_ms,
        )

    def restore_from(
        self,
        source_path: str | os.PathLike[str],
        *,
        pages_per_step: int = 256,
        sleep_ms: int = 0,
    ) -> None:
        """Perform an online restore from a source database file."""
        self._require_client().restore_from(
            str(source_path),
            pages_per_step=pages_per_step,
            sleep_ms=sleep_ms,
        )

    def last_insert_rowid(self) -> int | None:
        """Return the last inserted SQLite rowid."""
        raw_id = self._require_client().last_insert_rowid
        return int(raw_id) if raw_id > 0 else None
