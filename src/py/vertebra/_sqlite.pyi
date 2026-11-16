"""Type stubs for the vertebra._sqlite native extension module."""

from collections.abc import Awaitable, Mapping, Sequence
from typing import Any, final

from vertebra._vertebra import ArrowCursor

__version__: str
FREE_THREADED: bool
GIL_USED: bool

__all__ = [
    "AsyncSQLiteClient",
    "FREE_THREADED",
    "GIL_USED",
    "SyncSQLiteClient",
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

    def fetch_all(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> tuple[list[str], list[tuple[Any, ...]]]:
        """Execute a query and return (columns, rows)."""
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

    def fetch_all(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[tuple[list[str], list[tuple[Any, ...]]]]:
        """Execute a query asynchronously and return (columns, rows)."""
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
