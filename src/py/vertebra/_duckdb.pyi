"""Type stubs for the vertebra._duckdb native extension module."""

from collections.abc import Awaitable, Mapping, Sequence
from typing import Any, final

from vertebra._vertebra import ArrowCursor

__version__: str
FREE_THREADED: bool
GIL_USED: bool

__all__ = [
    "FREE_THREADED",
    "GIL_USED",
    "AsyncDuckDBClient",
    "AsyncDuckDBCursor",
    "SyncDuckDBClient",
    "SyncDuckDBCursor",
    "__version__",
    "broker_engine_capsule",
]

def broker_engine_capsule() -> object: ...

@final
class SyncDuckDBClient:
    """Synchronous native DuckDB 2.0 client bridged via SyncCompletionQueue."""

    def __new__(
        cls,
        database: str = ":memory:",
        *,
        read_only: bool = False,
        max_connections: int = 4,
        max_memory_bytes: int | None = None,
        threads: int | None = None,
        temp_directory: str | None = None,
        parquet_metadata_cache: bool = True,
        httpfs_connection_caching: bool = True,
        enable_http_metadata_cache: bool = True,
        config: Mapping[str, str] | None = None,
        extensions: Sequence[str] | None = None,
        init_statements: Sequence[str] | None = None,
    ) -> SyncDuckDBClient:
        """Initialize a synchronous DuckDB worker pool handle."""
        ...
    @property
    def database(self) -> str:
        """Return the resolved DuckDB database path or `:memory:`."""
        ...
    @property
    def read_only(self) -> bool:
        """Return whether the worker pool was opened in read-only mode."""
        ...
    @property
    def max_connections(self) -> int:
        """Return the configured number of pooled DuckDB worker connections."""
        ...
    @property
    def is_in_memory(self) -> bool:
        """Return whether the client targets an in-memory DuckDB catalog."""
        ...
    @property
    def in_transaction(self) -> bool:
        """Return whether an explicit transaction is active on the writer session."""
        ...
    @property
    def closed(self) -> bool:
        """Return whether the worker pool has been closed."""
        ...
    @property
    def bootstrap_statements(self) -> list[str]:
        """Return the session bootstrap `SET` statements applied to pooled workers."""
        ...
    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> int:
        """Execute a DDL or DML statement and return the affected row count."""
        ...
    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int:
        """Execute a statement across multiple parameter sets and return total affected rows."""
        ...
    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65536,
        threshold_rows: int = 0,
    ) -> ArrowCursor:
        """Stream analytical query results into an `ArrowCursor`."""
        ...
    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65536,
    ) -> list[ArrowCursor]:
        """Stream Arrow record batches as shared-memory `ArrowCursor` frames."""
        ...
    def load_from_arrow(
        self,
        table: str,
        source: Any,
        *,
        columns: Sequence[str] | None = None,
        create_if_missing: bool = True,
    ) -> int:
        """Bulk-ingest an Arrow IPC buffer, `ArrowCursor`, PyArrow table, or row sequence via `Appender`."""
        ...
    def begin(self) -> None:
        """Begin an explicit transaction on the writer worker."""
        ...
    def commit(self) -> None:
        """Commit the active transaction on the writer worker."""
        ...
    def rollback(self) -> None:
        """Roll back the active transaction on the writer worker."""
        ...
    def savepoint(self, name: str) -> None:
        """Record a transactional savepoint on the active transaction."""
        ...
    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back the active transaction to savepoint `name`."""
        ...
    def release_savepoint(self, name: str) -> None:
        """Release savepoint `name` on the active transaction."""
        ...
    def interrupt(self) -> None:
        """Interrupt any in-flight DuckDB query across pooled workers."""
        ...
    def cancel(self) -> None:
        """Interrupt any in-flight DuckDB query across pooled workers."""
        ...
    def close(self) -> None:
        """Shut down all pooled DuckDB worker threads."""
        ...
    def to_async(self) -> AsyncDuckDBClient:
        """Return an `AsyncDuckDBClient` sharing the same worker pool."""
        ...
    def cursor(self, *, lazy_begin: bool = False) -> SyncDuckDBCursor:
        """Return a PEP 249 cursor sharing the underlying worker pool."""
        ...
    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run one statement and return its rows and column names."""
        ...
    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> list[dict[str, Any]]:
        """Run one statement and return each row as a dictionary keyed by column name."""
        ...
    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Any:
        """Run one statement and return the first column of the first row."""
        ...
    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> tuple[int, int | None]:
        """Run one statement and return the changed-row count and `None`."""
        ...
    def __repr__(self) -> str: ...
    def __enter__(self) -> SyncDuckDBClient: ...
    def __exit__(
        self,
        _exc_type: Any = None,
        _exc_val: Any = None,
        _exc_tb: Any = None,
    ) -> None: ...

@final
class SyncDuckDBCursor:
    """Synchronous PEP 249 cursor over a native DuckDB worker pool."""

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
        """Return `None`; DuckDB reports no inserted rowid."""
        ...

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = ()) -> SyncDuckDBCursor:
        """Run one statement and buffer its result on this cursor."""
        ...

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]]) -> SyncDuckDBCursor:
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
class AsyncDuckDBClient:
    """Asynchronous native DuckDB 2.0 client bridged via AsyncCompletionQueue and Notifier."""

    def __new__(
        cls,
        database: str = ":memory:",
        *,
        read_only: bool = False,
        max_connections: int = 4,
        max_memory_bytes: int | None = None,
        threads: int | None = None,
        temp_directory: str | None = None,
        parquet_metadata_cache: bool = True,
        httpfs_connection_caching: bool = True,
        enable_http_metadata_cache: bool = True,
        config: Mapping[str, str] | None = None,
        extensions: Sequence[str] | None = None,
        init_statements: Sequence[str] | None = None,
    ) -> AsyncDuckDBClient:
        """Initialize an asynchronous DuckDB worker pool handle."""
        ...
    @property
    def database(self) -> str:
        """Return the resolved DuckDB database path or `:memory:`."""
        ...
    @property
    def read_only(self) -> bool:
        """Return whether the worker pool was opened in read-only mode."""
        ...
    @property
    def max_connections(self) -> int:
        """Return the configured number of pooled DuckDB worker connections."""
        ...
    @property
    def is_in_memory(self) -> bool:
        """Return whether the client targets an in-memory DuckDB catalog."""
        ...
    @property
    def in_transaction(self) -> bool:
        """Return whether an explicit transaction is active on the writer session."""
        ...
    @property
    def closed(self) -> bool:
        """Return whether the worker pool has been closed."""
        ...
    @property
    def bootstrap_statements(self) -> list[str]:
        """Return the session bootstrap `SET` statements applied to pooled workers."""
        ...
    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[int]:
        """Execute a DDL or DML statement asynchronously and return the affected row count."""
        ...
    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> Awaitable[int]:
        """Execute a statement across multiple parameter sets asynchronously."""
        ...
    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65536,
        threshold_rows: int = 0,
    ) -> Awaitable[ArrowCursor]:
        """Stream analytical query results asynchronously into an `ArrowCursor`."""
        ...
    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65536,
    ) -> Awaitable[list[ArrowCursor]]:
        """Stream Arrow record batches asynchronously as shared-memory `ArrowCursor` frames."""
        ...
    def load_from_arrow(
        self,
        table: str,
        source: Any,
        *,
        columns: Sequence[str] | None = None,
        create_if_missing: bool = True,
    ) -> Awaitable[int]:
        """Bulk-ingest an Arrow IPC buffer, `ArrowCursor`, PyArrow table, or row sequence asynchronously."""
        ...
    def begin(self) -> Awaitable[None]:
        """Begin an explicit transaction asynchronously."""
        ...
    def commit(self) -> Awaitable[None]:
        """Commit the active transaction asynchronously."""
        ...
    def rollback(self) -> Awaitable[None]:
        """Roll back the active transaction asynchronously."""
        ...
    def savepoint(self, name: str) -> Awaitable[None]:
        """Record a transactional savepoint asynchronously."""
        ...
    def rollback_to_savepoint(self, name: str) -> Awaitable[None]:
        """Roll back the active transaction asynchronously to savepoint `name`."""
        ...
    def release_savepoint(self, name: str) -> Awaitable[None]:
        """Release savepoint `name` asynchronously."""
        ...
    def interrupt(self) -> None:
        """Interrupt any in-flight DuckDB query across pooled workers."""
        ...
    def cancel(self) -> None:
        """Interrupt any in-flight DuckDB query across pooled workers."""
        ...
    def close(self) -> Awaitable[None]:
        """Shut down all pooled DuckDB worker threads asynchronously."""
        ...
    def to_sync(self) -> SyncDuckDBClient:
        """Return a `SyncDuckDBClient` sharing the same worker pool."""
        ...
    def cursor(self, *, lazy_begin: bool = False) -> AsyncDuckDBCursor:
        """Return an awaitable PEP 249-shaped cursor sharing the underlying worker pool."""
        ...
    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Awaitable[tuple[list[tuple[Any, ...]], tuple[str, ...]]]:
        """Run one statement asynchronously and return the rows alongside their column names."""
        ...
    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Awaitable[list[dict[str, Any]]]:
        """Run one statement asynchronously and return each row as a dictionary keyed by column name."""
        ...
    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Awaitable[Any]:
        """Run one statement asynchronously and return the first column of the first row."""
        ...
    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Awaitable[tuple[int, int | None]]:
        """Run one statement asynchronously and return the changed-row count and `None`."""
        ...
    def __repr__(self) -> str: ...
    def __aenter__(self) -> Awaitable[AsyncDuckDBClient]: ...
    def __aexit__(
        self,
        _exc_type: Any = None,
        _exc_val: Any = None,
        _exc_tb: Any = None,
    ) -> Awaitable[None]: ...

@final
class AsyncDuckDBCursor:
    """Awaitable PEP 249-shaped cursor over a native DuckDB worker pool."""

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
        """Return `None`; DuckDB reports no inserted rowid."""
        ...

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = ()) -> Awaitable[AsyncDuckDBCursor]:
        """Run one statement and buffer its result on this cursor."""
        ...

    def executemany(
        self, sql: str, seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]]
    ) -> Awaitable[AsyncDuckDBCursor]:
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
