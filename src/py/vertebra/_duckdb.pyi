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
    "SyncDuckDBClient",
    "__version__",
]

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
    def fetch_all(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a query and return all rows as dictionaries."""
        ...
    def fetch_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a query and return exactly one row dictionary, raising `NotFoundError` when empty."""
        ...
    def fetch_optional(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Execute a query and return a single row dictionary or `None` when empty."""
        ...
    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional row tuples."""
        ...
    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)` preserving column metadata on empty results."""
        ...
    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Any:
        """Execute a scalar query and return the first column of the first row."""
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
    def __repr__(self) -> str: ...
    def __enter__(self) -> SyncDuckDBClient: ...
    def __exit__(
        self,
        _exc_type: Any = None,
        _exc_val: Any = None,
        _exc_tb: Any = None,
    ) -> None: ...

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
    def fetch_all(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[list[dict[str, Any]]]:
        """Execute a query asynchronously and return all rows as dictionaries."""
        ...
    def fetch_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any]]:
        """Execute a query asynchronously and return one row dictionary, raising `NotFoundError` when empty."""
        ...
    def fetch_optional(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any] | None]:
        """Execute a query asynchronously and return one row dictionary or `None` when empty."""
        ...
    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[list[tuple[Any, ...]]]:
        """Execute a query asynchronously and return positional row tuples."""
        ...
    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[tuple[list[tuple[Any, ...]], tuple[str, ...]]]:
        """Execute SQL asynchronously and return `(rows, column_names)`."""
        ...
    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[Any]:
        """Execute a scalar query asynchronously and return the first column of the first row."""
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
    def __repr__(self) -> str: ...
    def __aenter__(self) -> Awaitable[AsyncDuckDBClient]: ...
    def __aexit__(
        self,
        _exc_type: Any = None,
        _exc_val: Any = None,
        _exc_tb: Any = None,
    ) -> Awaitable[None]: ...
