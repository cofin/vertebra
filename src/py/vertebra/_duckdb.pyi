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
    ) -> SyncDuckDBClient: ...
    @property
    def database(self) -> str: ...
    @property
    def read_only(self) -> bool: ...
    @property
    def max_connections(self) -> int: ...
    @property
    def is_in_memory(self) -> bool: ...
    @property
    def in_transaction(self) -> bool: ...
    @property
    def closed(self) -> bool: ...
    @property
    def applied_token_generation(self) -> int: ...
    @property
    def last_gcs_secret_sql(self) -> str | None: ...
    @property
    def bootstrap_statements(self) -> list[str]: ...
    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> int: ...
    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int: ...
    def fetch_all(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]: ...
    def fetch_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> dict[str, Any]: ...
    def fetch_optional(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None: ...
    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Any: ...
    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65536,
    ) -> ArrowCursor: ...
    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65536,
    ) -> list[ArrowCursor]: ...
    def load_from_arrow(
        self,
        table: str,
        source: Any,
        *,
        columns: Sequence[str] | None = None,
        create_if_missing: bool = True,
    ) -> int: ...
    def apply_gcs_secret(
        self,
        *,
        secret_name: str = "vtb_gcs_secret",
        token: str | None = None,
        generation: int | None = None,
        token_cache: Any | None = None,
        scope: str | None = None,
    ) -> bool: ...
    def begin(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def interrupt(self) -> None: ...
    def cancel(self) -> None: ...
    def close(self) -> None: ...
    def to_async(self) -> AsyncDuckDBClient: ...
    def __enter__(self) -> SyncDuckDBClient: ...
    def __exit__(
        self,
        _exc_type: Any = None,
        _exc_val: Any = None,
        _exc_tb: Any = None,
    ) -> None: ...

@final
class AsyncDuckDBClient:
    """Asynchronous native DuckDB 2.0 client bridged via AsyncCompletionQueue + Notifier."""

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
    ) -> AsyncDuckDBClient: ...
    @property
    def database(self) -> str: ...
    @property
    def read_only(self) -> bool: ...
    @property
    def max_connections(self) -> int: ...
    @property
    def is_in_memory(self) -> bool: ...
    @property
    def in_transaction(self) -> bool: ...
    @property
    def closed(self) -> bool: ...
    @property
    def applied_token_generation(self) -> int: ...
    @property
    def last_gcs_secret_sql(self) -> str | None: ...
    @property
    def bootstrap_statements(self) -> list[str]: ...
    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[int]: ...
    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> Awaitable[int]: ...
    def fetch_all(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[list[dict[str, Any]]]: ...
    def fetch_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any]]: ...
    def fetch_optional(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any] | None]: ...
    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Awaitable[Any]: ...
    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65536,
    ) -> Awaitable[ArrowCursor]: ...
    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65536,
    ) -> Awaitable[list[ArrowCursor]]: ...
    def load_from_arrow(
        self,
        table: str,
        source: Any,
        *,
        columns: Sequence[str] | None = None,
        create_if_missing: bool = True,
    ) -> Awaitable[int]: ...
    def apply_gcs_secret(
        self,
        *,
        secret_name: str = "vtb_gcs_secret",
        token: str | None = None,
        generation: int | None = None,
        token_cache: Any | None = None,
        scope: str | None = None,
    ) -> Awaitable[bool]: ...
    def begin(self) -> Awaitable[None]: ...
    def commit(self) -> Awaitable[None]: ...
    def rollback(self) -> Awaitable[None]: ...
    def interrupt(self) -> None: ...
    def cancel(self) -> None: ...
    def close(self) -> Awaitable[None]: ...
    def to_sync(self) -> SyncDuckDBClient: ...
