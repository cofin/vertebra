"""Consolidated protocols for Vertebra.

This module provides all protocol definitions organized by domain:
- Drivers: Database connections, cursors, and pools
- Serializers: JSON and general serialization
- Context managers: Sync and async context manager protocols
- Application: Provider plugin protocols
- Data frames: NumPy, Pandas, and Polars protocols
- Arrow: PyArrow type protocols

Note: Storage protocols remain in vertebra.storage.store due to their
tight coupling with data types defined there.

All protocols use @runtime_checkable for isinstance() checks at runtime.
"""

from typing import TYPE_CHECKING, Any, Protocol, TypeVar, overload, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterable, Iterator, Sequence

# ============================================================================
# Type Variables
# ============================================================================

_ConnectionT_co = TypeVar("_ConnectionT_co", bound="SyncConnection | AsyncConnection", covariant=True)
T = TypeVar("T")


# ============================================================================
# Driver Protocols
# ============================================================================


@runtime_checkable
class SyncCursor(Protocol):
    """DB-API cursor protocol."""

    def execute(self, query: str, params: "Sequence[Any] | None" = None) -> Any:
        """Execute a query and return result metadata."""

    def executemany(self, query: str, params: "Iterable[Sequence[Any]]") -> Any:
        """Execute a query for many parameter sets."""

    def fetchone(self) -> Any:
        """Fetch a single row."""

    def fetchmany(self, size: int | None = None) -> list[Any]:
        """Fetch multiple rows."""

    def fetchall(self) -> list[Any]:
        """Fetch all remaining rows."""

    def close(self) -> None:
        """Close the cursor."""


@runtime_checkable
class AsyncCursor(Protocol):
    """Async cursor protocol."""

    async def execute(self, query: str, params: "Sequence[Any] | None" = None) -> Any:
        """Execute a query asynchronously and return result metadata."""

    async def executemany(self, query: str, params: "Iterable[Sequence[Any]]") -> Any:
        """Execute a query for many parameter sets asynchronously."""

    async def fetchone(self) -> Any:
        """Fetch a single row asynchronously."""

    async def fetchmany(self, size: int | None = None) -> list[Any]:
        """Fetch multiple rows asynchronously."""

    async def fetchall(self) -> list[Any]:
        """Fetch all remaining rows asynchronously."""

    async def close(self) -> None:
        """Close the cursor."""


@runtime_checkable
class SyncConnection(Protocol):
    """DB-API connection protocol."""

    def cursor(self) -> SyncCursor:
        """Create a new cursor."""

    def commit(self) -> None:
        """Commit the current transaction."""

    def rollback(self) -> None:
        """Rollback the current transaction."""

    def close(self) -> None:
        """Close the connection."""


@runtime_checkable
class AsyncConnection(Protocol):
    """Async driver connection protocol."""

    def cursor(self) -> AsyncCursor:
        """Create a new async cursor."""

    async def commit(self) -> None:
        """Commit the current transaction."""

    async def rollback(self) -> None:
        """Rollback the current transaction."""

    async def fetch(self, query: str, params: "Sequence[Any] | None" = None) -> list[Any]:
        """Fetch many rows asynchronously."""

    async def fetchrow(self, query: str, params: "Sequence[Any] | None" = None) -> Any:
        """Fetch a single row asynchronously."""

    async def fetchval(self, query: str, params: "Sequence[Any] | None" = None) -> Any:
        """Fetch a single value asynchronously."""

    async def execute(self, query: str, params: "Sequence[Any] | None" = None) -> Any:
        """Execute a query asynchronously."""

    async def executemany(self, query: str, params: "Iterable[Sequence[Any]]") -> Any:
        """Execute a query for many parameter sets asynchronously."""

    async def close(self) -> None:
        """Close the connection."""


@runtime_checkable
class SyncPool(Protocol[_ConnectionT_co]):
    """Connection pool protocol."""

    def acquire(self) -> _ConnectionT_co:
        """Acquire a connection from the pool."""

    def release(self, connection: Any) -> None:
        """Release a connection back to the pool."""

    def close(self) -> None:
        """Close the pool."""

    def get_size(self) -> int:
        """Total pool size."""

    def get_idle_size(self) -> int:
        """Idle connection count."""


@runtime_checkable
class AsyncPool(Protocol[_ConnectionT_co]):
    """Async connection pool protocol."""

    async def acquire(self) -> _ConnectionT_co:
        """Acquire a connection from the pool asynchronously."""

    async def release(self, connection: Any) -> None:
        """Release a connection back to the pool asynchronously."""

    async def close(self) -> None:
        """Close the pool asynchronously."""

    def get_size(self) -> int:
        """Total pool size."""

    def get_idle_size(self) -> int:
        """Idle connection count."""


# ============================================================================
# Serializer Protocols
# ============================================================================


@runtime_checkable
class JSONSerializer(Protocol):
    """Protocol for JSON serialization implementations."""

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode data to JSON."""
        ...

    def decode(self, data: str | bytes) -> Any:
        """Decode from JSON."""
        ...


@runtime_checkable
class Serializer(Protocol):
    """Protocol for serialization backends."""

    def serialize(self, value: Any, type_hint: type | None = None) -> bytes:
        """Serialize a Python value to bytes."""
        ...

    @overload
    def deserialize(self, data: bytes, type_hint: type[T]) -> T: ...

    @overload
    def deserialize(self, data: bytes, type_hint: None = ...) -> Any: ...

    def deserialize(self, data: bytes, type_hint: type[Any] | None = None) -> Any:
        """Deserialize bytes to a Python value."""

    def get_type_metadata(self, type_hint: type | None = None) -> Any:
        """Extract type metadata for Type IR."""


# ============================================================================
# Context Manager Protocols
# ============================================================================


@runtime_checkable
class SyncContextManager(Protocol):
    """Protocol for synchronous context managers with proper exit typing."""

    def __enter__(self) -> Any: ...

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
    ) -> bool | None: ...


@runtime_checkable
class AsyncContextManager(Protocol):
    """Protocol for asynchronous context managers with proper exit typing."""

    async def __aenter__(self) -> Any: ...

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
    ) -> bool | None: ...


# ============================================================================
# Application Protocols
# ============================================================================


@runtime_checkable
class ProviderPlugin(Protocol):
    """Protocol for broker plugins that register and serve providers."""

    def register(self, *args: Any, **kwargs: Any) -> "Awaitable[None]": ...

    def serve(self) -> "Awaitable[None]": ...


# ============================================================================
# Data Frame Protocols
# ============================================================================


@runtime_checkable
class NumpyArrayProtocol(Protocol):
    """Protocol for numpy.ndarray - works without numpy installed."""

    @property
    def shape(self) -> tuple[int, ...]: ...

    @property
    def dtype(self) -> Any: ...

    @property
    def ndim(self) -> int: ...

    def tolist(self) -> list[Any]: ...

    def __len__(self) -> int: ...


@runtime_checkable
class PandasDataFrameProtocol(Protocol):
    """Protocol for pandas.DataFrame - works without pandas installed."""

    @property
    def columns(self) -> Any: ...

    @property
    def index(self) -> Any: ...

    def to_dict(self, orient: str = "records") -> list[dict[str, Any]]: ...


@runtime_checkable
class PolarsDataFrameProtocol(Protocol):
    """Protocol for polars.DataFrame - works without polars installed."""

    @property
    def columns(self) -> list[str]: ...

    @property
    def schema(self) -> dict[str, Any]: ...

    def to_arrow(self) -> Any: ...

    def to_dicts(self) -> list[dict[str, Any]]: ...


# ============================================================================
# Arrow Protocols
# ============================================================================


@runtime_checkable
class ArrowFieldProtocol(Protocol):
    """Protocol for pyarrow.Field."""

    @property
    def name(self) -> str: ...

    @property
    def type(self) -> Any: ...

    @property
    def nullable(self) -> bool: ...


@runtime_checkable
class ArrowSchemaProtocol(Protocol):
    """Protocol for pyarrow.Schema."""

    @property
    def names(self) -> list[str]: ...

    def field(self, i: int | str) -> ArrowFieldProtocol: ...

    def __len__(self) -> int: ...

    def __iter__(self) -> "Iterator[ArrowFieldProtocol]": ...


@runtime_checkable
class ArrowTableProtocol(Protocol):
    """Protocol for pyarrow.Table."""

    @property
    def column_names(self) -> list[str]: ...

    @property
    def num_rows(self) -> int: ...

    @property
    def num_columns(self) -> int: ...

    @property
    def schema(self) -> ArrowSchemaProtocol: ...

    def to_pylist(self) -> list[dict[str, Any]]: ...

    def to_pydict(self) -> dict[str, Any]: ...

    def column(self, i: int | str) -> Any: ...

    def select(self, columns: list[str]) -> "ArrowTableProtocol": ...


@runtime_checkable
class ArrowRecordBatchProtocol(Protocol):
    """Protocol for pyarrow.RecordBatch."""

    @property
    def column_names(self) -> list[str]: ...

    @property
    def num_rows(self) -> int: ...

    @property
    def num_columns(self) -> int: ...

    @property
    def schema(self) -> ArrowSchemaProtocol: ...

    def to_pylist(self) -> list[dict[str, Any]]: ...

    def to_pydict(self) -> dict[str, Any]: ...


@runtime_checkable
class ArrowArrayProtocol(Protocol):
    """Protocol for pyarrow.Array."""

    def to_pylist(self) -> list[Any]: ...

    @property
    def type(self) -> Any: ...

    def __len__(self) -> int: ...


__all__ = (
    # Arrow protocols
    "ArrowArrayProtocol",
    "ArrowFieldProtocol",
    "ArrowRecordBatchProtocol",
    "ArrowSchemaProtocol",
    "ArrowTableProtocol",
    # Driver protocols
    "AsyncConnection",
    # Context manager protocols
    "AsyncContextManager",
    "AsyncCursor",
    "AsyncPool",
    # Serializer protocols
    "JSONSerializer",
    # Data frame protocols
    "NumpyArrayProtocol",
    "PandasDataFrameProtocol",
    "PolarsDataFrameProtocol",
    # Application protocols
    "ProviderPlugin",
    "Serializer",
    "SyncConnection",
    "SyncContextManager",
    "SyncCursor",
    "SyncPool",
)
