"""Type stubs for the vertebra._sqlite native extension module."""

from collections.abc import Awaitable, Mapping, Sequence
from typing import Any, final

from vertebra._vertebra import ArrowCursor

__version__: str
FREE_THREADED: bool
GIL_USED: bool

__all__ = [
    "AsyncSQLiteClient",
    "AsyncSQLiteCursor",
    "FREE_THREADED",
    "GIL_USED",
    "SyncSQLiteClient",
    "SyncSQLiteCursor",
    "__version__",
    "broker_engine_capsule",
]

def broker_engine_capsule() -> object:
    """Return a PyCapsule wrapping a BrokerPoolBackend for SQLite."""
    ...

@final
class SyncSQLiteClient:
    """Synchronous native SQLite 3.45+ client bridged via dedicated worker threads."""

    def __new__(
        cls,
        path: str = ":memory:",
        *,
        wal_mode: bool = True,
        synchronous: str = "NORMAL",
        mmap_size: int = 268435456,
        busy_timeout_ms: int = 5000,
        max_readers: int = 8,
        foreign_keys: bool = True,
        enable_jsonb: bool = True,
    ) -> SyncSQLiteClient:
        """Initialize a synchronous SQLite worker pool handle."""
        ...

    @property
    def path(self) -> str:
        """Return the resolved SQLite database path or `:memory:`."""
        ...

    @property
    def in_transaction(self) -> bool:
        """Return whether an explicit transaction is active on the writer connection."""
        ...

    @property
    def closed(self) -> bool:
        """Return whether the client worker threads have been shut down."""
        ...

    @property
    def last_insert_rowid(self) -> int:
        """Return the rowid of the most recent successful INSERT on the writer connection."""
        ...

    @property
    def last_statement_rowid(self) -> int | None:
        """Return the rowid of the last statement executed through a single-call method, or `None`."""
        ...

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> int:
        """Execute a DDL or DML statement and return the affected row count."""
        ...

    def execute_many(
        self,
        sql: str,
        params_seq: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int:
        """Execute a statement across multiple parameter sequences and return total affected rows."""
        ...

    def execute_script(self, sql_script: str) -> None:
        """Execute multiple SQL statements separated by semicolons."""
        ...

    def execute_pipeline(
        self,
        operations: Sequence[tuple[bool, str, Sequence[Any] | Mapping[str, Any] | None]],
        *,
        use_savepoint: bool = False,
    ) -> list[Any]:
        """Execute a sequence of operations atomically within a batch transaction."""
        ...

    def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
    ) -> ArrowCursor:
        """Stream query results into an ArrowCursor backed by VTBARROW shared memory."""
        ...

    def load_from_arrow(self, table_name: str, arrow_ipc_bytes: bytes) -> int:
        """Ingest Arrow IPC stream bytes into a database table."""
        ...

    def backup_to(
        self,
        target_path: str,
        *,
        pages_per_step: int = 256,
        sleep_ms: int = 0,
    ) -> None:
        """Perform an online backup to a target database file."""
        ...

    def restore_from(
        self,
        source_path: str,
        *,
        pages_per_step: int = 256,
        sleep_ms: int = 0,
    ) -> None:
        """Restore the database from a source database file."""
        ...

    def begin(self, mode: str = "IMMEDIATE") -> None:
        """Begin an explicit SQLite transaction."""
        ...

    def commit(self) -> None:
        """Commit the active transaction."""
        ...

    def rollback(self) -> None:
        """Roll back the active transaction."""
        ...

    def cancel(self) -> None:
        """Interrupt any in-flight SQLite query on this connection."""
        ...

    def close(self) -> None:
        """Shut down worker threads and close database connections."""
        ...

    def to_async(self) -> AsyncSQLiteClient:
        """Return an asynchronous SQLite client sharing the underlying worker pool."""
        ...

    def cursor(self, *, lazy_begin: bool = False) -> SyncSQLiteCursor:
        """Return a PEP 249 cursor sharing the underlying worker pool."""
        ...

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a statement and return rows as tuples paired with column names."""
        ...

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> list[dict[str, Any]]:
        """Execute a statement and return rows as dictionaries keyed by column name."""
        ...

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Any:
        """Execute a statement and return the first column of the first row."""
        ...

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> tuple[int, int | None]:
        """Execute a statement and return affected row count and inserted rowid."""
        ...

@final
class SyncSQLiteCursor:
    """Synchronous PEP 249 cursor over a native SQLite worker pool."""

    arraysize: int

    @property
    def description(self) -> tuple[tuple[str, None, None, None, None, None, None], ...] | None:
        """Return the column descriptions of the current result set, or `None`."""
        ...

    @property
    def rowcount(self) -> int:
        """Return the changed-row count of the last statement, or `-1`."""
        ...

    @property
    def lastrowid(self) -> int | None:
        """Return the rowid inserted by the last statement, or `None`."""
        ...

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = ()) -> SyncSQLiteCursor:
        """Run one statement and buffer its result on this cursor."""
        ...

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]]) -> SyncSQLiteCursor:
        """Run one statement once per parameter set."""
        ...

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next buffered row, or `None` when the result set is exhausted."""
        ...

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Return up to `size` buffered rows, defaulting to `arraysize`."""
        ...

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return every remaining buffered row."""
        ...

    def close(self) -> None:
        """Close the cursor and drop its buffered rows."""
        ...

@final
class AsyncSQLiteClient:
    """Asynchronous native SQLite 3.45+ client bridged via Tokio and completion drainer."""

    def __new__(
        cls,
        path: str = ":memory:",
        *,
        wal_mode: bool = True,
        synchronous: str = "NORMAL",
        mmap_size: int = 268435456,
        busy_timeout_ms: int = 5000,
        max_readers: int = 8,
        foreign_keys: bool = True,
        enable_jsonb: bool = True,
    ) -> AsyncSQLiteClient:
        """Initialize an asynchronous SQLite worker pool handle."""
        ...

    @property
    def path(self) -> str:
        """Return the resolved SQLite database path or `:memory:`."""
        ...

    @property
    def in_transaction(self) -> bool:
        """Return whether an explicit transaction is active on the writer connection."""
        ...

    @property
    def closed(self) -> bool:
        """Return whether the client worker threads have been shut down."""
        ...

    @property
    def last_insert_rowid(self) -> int:
        """Return the rowid of the most recent successful INSERT on the writer connection."""
        ...

    @property
    def last_statement_rowid(self) -> int | None:
        """Return the rowid of the last statement executed through a single-call method, or `None`."""
        ...

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[int]:
        """Execute a DDL or DML statement asynchronously."""
        ...

    def execute_many(
        self,
        sql: str,
        params_seq: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> Awaitable[int]:
        """Execute a statement across multiple parameter sequences asynchronously."""
        ...

    def execute_script(self, sql_script: str) -> Awaitable[None]:
        """Execute multiple SQL statements asynchronously."""
        ...

    def execute_pipeline(
        self,
        operations: Sequence[tuple[bool, str, Sequence[Any] | Mapping[str, Any] | None]],
        *,
        use_savepoint: bool = False,
    ) -> Awaitable[list[Any]]:
        """Execute a sequence of operations atomically within a batch transaction asynchronously."""
        ...

    def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
    ) -> Awaitable[ArrowCursor]:
        """Stream query results asynchronously into an ArrowCursor."""
        ...

    def load_from_arrow(self, table_name: str, arrow_ipc_bytes: bytes) -> Awaitable[int]:
        """Ingest Arrow IPC stream bytes into a database table asynchronously."""
        ...

    def backup_to(
        self,
        target_path: str,
        *,
        pages_per_step: int = 256,
        sleep_ms: int = 0,
    ) -> Awaitable[None]:
        """Perform an online backup to a target database file asynchronously."""
        ...

    def restore_from(
        self,
        source_path: str,
        *,
        pages_per_step: int = 256,
        sleep_ms: int = 0,
    ) -> Awaitable[None]:
        """Restore the database from a source database file asynchronously."""
        ...

    def begin(self, mode: str = "IMMEDIATE") -> Awaitable[None]:
        """Begin an explicit SQLite transaction asynchronously."""
        ...

    def commit(self) -> Awaitable[None]:
        """Commit the active transaction asynchronously."""
        ...

    def rollback(self) -> Awaitable[None]:
        """Roll back the active transaction asynchronously."""
        ...

    def cancel(self) -> None:
        """Interrupt any in-flight SQLite query on this connection."""
        ...

    def close(self) -> Awaitable[None]:
        """Shut down worker threads and close database connections asynchronously."""
        ...

    def to_sync(self) -> SyncSQLiteClient:
        """Return a synchronous SQLite client sharing the underlying worker pool."""
        ...

    def cursor(self, *, lazy_begin: bool = False) -> AsyncSQLiteCursor:
        """Return an awaitable cursor sharing the underlying worker pool."""
        ...

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Awaitable[tuple[list[tuple[Any, ...]], tuple[str, ...]]]:
        """Execute a statement asynchronously and return rows as tuples paired with column names."""
        ...

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Awaitable[list[dict[str, Any]]]:
        """Execute a statement asynchronously and return rows as dictionaries keyed by column name."""
        ...

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Awaitable[Any]:
        """Execute a statement asynchronously and return the first column of the first row."""
        ...

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Awaitable[tuple[int, int | None]]:
        """Execute a statement asynchronously and return affected row count and inserted rowid."""
        ...

@final
class AsyncSQLiteCursor:
    """Awaitable PEP 249-shaped cursor over a native SQLite worker pool."""

    arraysize: int

    @property
    def description(self) -> tuple[tuple[str, None, None, None, None, None, None], ...] | None:
        """Return the column descriptions of the current result set, or `None`."""
        ...

    @property
    def rowcount(self) -> int:
        """Return the changed-row count of the last statement, or `-1`."""
        ...

    @property
    def lastrowid(self) -> int | None:
        """Return the rowid inserted by the last statement, or `None`."""
        ...

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = ()) -> Awaitable[AsyncSQLiteCursor]:
        """Run one statement and buffer its result on this cursor."""
        ...

    def executemany(
        self, sql: str, seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]]
    ) -> Awaitable[AsyncSQLiteCursor]:
        """Run one statement once per parameter set."""
        ...

    def fetchone(self) -> Awaitable[tuple[Any, ...] | None]:
        """Return the next buffered row, or `None` when the result set is exhausted."""
        ...

    def fetchmany(self, size: int | None = None) -> Awaitable[list[tuple[Any, ...]]]:
        """Return up to `size` buffered rows, defaulting to `arraysize`."""
        ...

    def fetchall(self) -> Awaitable[list[tuple[Any, ...]]]:
        """Return every remaining buffered row."""
        ...

    def close(self) -> Awaitable[None]:
        """Close the cursor and drop its buffered rows."""
        ...
