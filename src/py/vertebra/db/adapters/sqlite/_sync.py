"""Synchronous SQLite WAL driver backed by `vertebra._sqlite`."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

from vertebra._vertebra import classify_sql_statement
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
from vertebra.utils.text import quote_dialect_identifier, validate_identifier

if TYPE_CHECKING:
    from vertebra._sqlite import SyncSQLiteClient

__all__ = ("SyncSQLiteDriver",)


def _sqlite_param_count(parameters: Any) -> int:
    """Return parameter count for sequence or mapping bind payloads."""
    if isinstance(parameters, (Sequence, Mapping)) and not isinstance(parameters, (str, bytes)):
        return len(parameters)
    return 0


class SyncSQLiteDriver(SyncDatabaseDriver[SQLiteConfig]):
    """Synchronous SQLite WAL driver backed by native `vertebra._sqlite.SyncSQLiteClient`."""

    dialect: ClassVar[str] = "sqlite"
    dialect_tag: ClassVar[int] = SQLITE_DIALECT_TAG

    __slots__ = ("_client", "_lastrowid")

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
        self._lastrowid: int | None = None
        if _client is not None:
            self._client: SyncSQLiteClient | None = _client
        else:
            self._client = self._create_client()

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
        return self._lastrowid

    def connect(self) -> Self:
        """Establish or reopen the native SQLite client connection."""
        if self._client is None or self._closed:
            self._client = self._create_client()
            self._closed = False
            self._lastrowid = None
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
        self._lastrowid = None
        if self._client is not None:
            client = self._client
            self._client = None
            client.close()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        rowcount, _ = self.execute_dml(sql, parameters, **kwargs)
        return rowcount

    def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[int, int | None]:
        """Execute a DML or DDL statement and atomically return (rowcount, lastrowid)."""
        _ = kwargs
        _, is_insert, _ = classify_sql_statement(sql)
        self._lookup_or_cache_statement(sql, param_count=_sqlite_param_count(parameters))
        lease = self._acquire_broker_lease()
        try:
            rowcount = self._require_client().execute(sql, parameters)
            rowid = None
            if is_insert and rowcount > 0:
                raw_id = self._require_client().last_insert_rowid
                if raw_id > 0:
                    rowid = raw_id
            self._lastrowid = rowid
            return rowcount, rowid
        finally:
            self._release_broker_lease(lease)

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
        self._lookup_or_cache_statement(sql, param_count=_sqlite_param_count(parameter_sets[0]))
        lease = self._acquire_broker_lease()
        try:
            return self._require_client().execute_many(sql, parameter_sets)
        finally:
            self._release_broker_lease(lease)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return unboxed tuple rows."""
        rows, _ = self.execute_with_description(sql, parameters, **kwargs)
        return rows

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return (tuple_rows, column_names)."""
        _ = kwargs
        _, is_insert, _ = classify_sql_statement(sql)
        self._lookup_or_cache_statement(sql, param_count=_sqlite_param_count(parameters))
        lease = self._acquire_broker_lease()
        try:
            cols, rows = self._require_client().fetch_all(sql, parameters)
            if is_insert:
                raw_id = self._require_client().last_insert_rowid
                self._lastrowid = int(raw_id) if raw_id > 0 else None
            else:
                self._lastrowid = None
            return list(rows), tuple(str(c) for c in cols)
        finally:
            self._release_broker_lease(lease)

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a SELECT query and hydrate rows into dictionaries or schema_type instances."""
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        return self.hydrate_rows(rows, cols, schema_type)

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
        self._lookup_or_cache_statement(sql, param_count=_sqlite_param_count(parameters))
        lease = self._acquire_broker_lease()
        try:
            native = self._require_client().select_to_arrow(sql, parameters)
            return native if isinstance(native, ArrowCursor) else ArrowCursor(native)
        finally:
            self._release_broker_lease(lease)

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
