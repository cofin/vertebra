"""Type stubs for the vertebra._spanner native extension module."""

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, TypedDict, final, type_check_only

@type_check_only
class SpannerFieldSchema(TypedDict):
    """Cloud Spanner query result column schema descriptor."""

    name: str
    """Result column name."""
    type: str
    """Cloud Spanner column type identifier."""

@type_check_only
class SpannerQueryResult(TypedDict):
    """Cloud Spanner query result payload."""

    columns: list[str]
    """Ordered output column names."""
    schema: list[SpannerFieldSchema]
    """Schema descriptors for result columns."""
    rows: list[list[Any]]
    """Decoded tabular result rows."""
    row_count_exact: int | None
    """Exact affected row count for DML statements."""
    row_count_lower_bound: int | None
    """Lower bound on affected row count for PDML statements."""

@type_check_only
class SpannerCommitResult(TypedDict):
    """Cloud Spanner transaction commit summary."""

    commit_timestamp_micros: int | None
    """Server-assigned commit timestamp in Unix microseconds."""
    mutation_count: int | None
    """Applied mutation count returned when commit stats are enabled."""
    retry_count: int
    """Number of transient retry attempts performed before commit succeeded."""

@type_check_only
class SpannerBatchWriteResult(TypedDict):
    """Cloud Spanner BatchWrite mutation group status payload."""

    indexes: list[int]
    """Indexes of mutation groups covered by this status entry."""
    status_code: int
    """gRPC status code for the mutation group batch."""
    status_message: str
    """Human-readable status message returned by Spanner."""
    commit_timestamp_micros: int | None
    """Commit timestamp in Unix microseconds when the batch succeeded."""

__version__: str
FREE_THREADED: bool
GIL_USED: bool

__all__ = [
    "FREE_THREADED",
    "GIL_USED",
    "AsyncSpannerClient",
    "AsyncSpannerCursor",
    "AsyncSpannerReadWriteTransaction",
    "SyncSpannerClient",
    "SyncSpannerCursor",
    "SyncSpannerReadWriteTransaction",
    "__version__",
]

@final
class SyncSpannerCursor:
    """Synchronous PEP 249 cursor backed by a Cloud Spanner client or transaction."""

    @property
    def description(self) -> tuple[tuple[str, str, None, None, None, None, None], ...] | None:
        """Return PEP 249 column metadata tuples, or `None` when no result set is active."""
        ...
    @property
    def rowcount(self) -> int:
        """Return the affected or buffered row count, or `-1` when unavailable."""
        ...
    @property
    def lastrowid(self) -> Any | None:
        """Return the last inserted row identifier, or `None` for Spanner."""
        ...
    @property
    def arraysize(self) -> int:
        """Return the default row batch size used by `fetchmany()`."""
        ...
    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """Set the default row batch size used by `fetchmany()`."""
        ...
    def execute(
        self,
        sql: str,
        params: str | Sequence[Any] | Mapping[str, Any] = (),
    ) -> SyncSpannerCursor:
        """Execute a query, DML, or DDL statement and buffer its result on this cursor."""
        ...
    def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[str | Sequence[Any] | Mapping[str, Any]],
    ) -> SyncSpannerCursor:
        """Execute a DML statement for each parameter set as one batch-DML call."""
        ...
    def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next buffered row tuple, or `None` when exhausted."""
        ...
    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Return up to `size` buffered row tuples, defaulting to `arraysize`."""
        ...
    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all remaining buffered row tuples."""
        ...
    def close(self) -> None:
        """Close the cursor idempotently and release buffered rows."""
        ...
    def __enter__(self) -> SyncSpannerCursor:
        """Enter a context manager block returning this cursor."""
        ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: Any | None = None,
    ) -> bool:
        """Exit a context manager block and close this cursor."""
        ...

@final
class AsyncSpannerCursor:
    """Asynchronous PEP 249 cursor backed by a Cloud Spanner client or transaction."""

    @property
    def description(self) -> tuple[tuple[str, str, None, None, None, None, None], ...] | None:
        """Return PEP 249 column metadata tuples, or `None` when no result set is active."""
        ...
    @property
    def rowcount(self) -> int:
        """Return the affected or buffered row count, or `-1` when unavailable."""
        ...
    @property
    def lastrowid(self) -> Any | None:
        """Return the last inserted row identifier, or `None` for Spanner."""
        ...
    @property
    def arraysize(self) -> int:
        """Return the default row batch size used by `fetchmany()`."""
        ...
    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """Set the default row batch size used by `fetchmany()`."""
        ...
    def execute(
        self,
        sql: str,
        params: str | Sequence[Any] | Mapping[str, Any] = (),
    ) -> Awaitable[AsyncSpannerCursor]:
        """Execute a query, DML, or DDL statement asynchronously and buffer its result."""
        ...
    def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[str | Sequence[Any] | Mapping[str, Any]],
    ) -> Awaitable[AsyncSpannerCursor]:
        """Execute a DML statement for each parameter set asynchronously as one batch-DML call."""
        ...
    def fetchone(self) -> Awaitable[tuple[Any, ...] | None]:
        """Return the next buffered row tuple asynchronously, or `None` when exhausted."""
        ...
    def fetchmany(self, size: int | None = None) -> Awaitable[list[tuple[Any, ...]]]:
        """Return up to `size` buffered row tuples asynchronously, defaulting to `arraysize`."""
        ...
    def fetchall(self) -> Awaitable[list[tuple[Any, ...]]]:
        """Return all remaining buffered row tuples asynchronously."""
        ...
    def close(self) -> Awaitable[None]:
        """Close the cursor idempotently and release buffered rows asynchronously."""
        ...

@final
class SyncSpannerReadWriteTransaction:
    """Stateful synchronous Cloud Spanner interactive ReadWrite transaction handle."""

    @property
    def transaction_id(self) -> bytes:
        """Return the server-assigned Cloud Spanner transaction identifier bytes."""
        ...
    def query(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> SpannerQueryResult:
        """Execute a SQL query inside this transaction and return result columns, schema, and rows."""
        ...
    def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> int:
        """Execute a DML statement inside this transaction and return the affected row count."""
        ...
    def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements inside this transaction and return per-statement row counts."""
        ...
    def commit(
        self,
        mutations_json: str | None = None,
    ) -> SpannerCommitResult:
        """Commit this transaction with optional buffered mutations and return commit statistics."""
        ...
    def rollback(self) -> None:
        """Explicitly roll back this transaction."""
        ...
    def cursor(
        self,
        *,
        request_tag: str | None = None,
        param_encoder: Callable[[Any], str | None] | None = None,
    ) -> SyncSpannerCursor:
        """Create a synchronous PEP 249 cursor bound to this transaction."""
        ...

@final
class AsyncSpannerReadWriteTransaction:
    """Stateful asynchronous Cloud Spanner interactive ReadWrite transaction handle."""

    @property
    def transaction_id(self) -> bytes:
        """Return the server-assigned Cloud Spanner transaction identifier bytes."""
        ...
    def query(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> Awaitable[SpannerQueryResult]:
        """Execute a SQL query inside this transaction asynchronously."""
        ...
    def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> Awaitable[int]:
        """Execute a DML statement inside this transaction asynchronously."""
        ...
    def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
    ) -> Awaitable[list[int]]:
        """Execute a batch of DML statements inside this transaction asynchronously."""
        ...
    def commit(
        self,
        mutations_json: str | None = None,
    ) -> Awaitable[SpannerCommitResult]:
        """Commit this transaction with optional buffered mutations asynchronously."""
        ...
    def rollback(self) -> Awaitable[None]:
        """Explicitly roll back this transaction asynchronously."""
        ...
    def cursor(
        self,
        *,
        request_tag: str | None = None,
        param_encoder: Callable[[Any], str | None] | None = None,
    ) -> AsyncSpannerCursor:
        """Create an asynchronous PEP 249 cursor bound to this transaction."""
        ...

@final
class SyncSpannerClient:
    """Synchronous Google Cloud Spanner client built on the official Rust client.

    ``query`` and ``partitioned_query`` return ``columns``, ``schema``, ``rows``,
    ``row_count_exact``, and ``row_count_lower_bound``. ``ca_pem`` is accepted
    only for non-googleapis ``https://`` endpoints (Spanner Omni or private test
    servers).
    """

    def __new__(
        cls,
        database: str,
        database_role: str | None = None,
        endpoint: str | None = None,
        dialect: str = "googlesql",
        pool_size: int = 4,
        route_to_leader_enabled: bool = True,
        max_commit_delay_ms: int | None = None,
        auth_options: dict[str, str] | None = None,
        admin_endpoint: str | None = None,
        ca_pem: bytes | None = None,
    ) -> SyncSpannerClient:
        """Initialize a synchronous Cloud Spanner client handle."""
        ...
    @property
    def database(self) -> str:
        """Return the full `projects/{p}/instances/{i}/databases/{d}` resource URI."""
        ...
    @property
    def database_role(self) -> str | None:
        """Return the configured fine-grained access control database role, if any."""
        ...
    @property
    def endpoint(self) -> str:
        """Return the effective Cloud Spanner Data API gRPC endpoint."""
        ...
    @property
    def admin_endpoint(self) -> str | None:
        """Return the configured Cloud Spanner Database Admin REST endpoint, if any."""
        ...
    @property
    def dialect(self) -> str:
        """Return the SQL dialect (`googlesql` or `spangres`)."""
        ...
    @property
    def pool_size(self) -> int:
        """Return the configured gRPC channel pool size."""
        ...
    @property
    def route_to_leader_enabled(self) -> bool:
        """Return whether leader-aware routing headers are enabled."""
        ...
    @property
    def max_commit_delay_ms(self) -> int | None:
        """Return the configured maximum commit batching delay in milliseconds, if any."""
        ...
    def query(
        self,
        sql: str,
        params_json: str | None = None,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> SpannerQueryResult:
        """Execute a streaming SQL read at the requested staleness bound."""
        ...
    def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> int:
        """Execute a transactional DML statement and return the affected row count."""
        ...
    def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements and return per-statement affected row counts."""
        ...
    def execute_partitioned_dml(
        self,
        sql: str,
        params_json: str | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> int:
        """Execute a Partitioned DML statement and return the lower-bound affected row count."""
        ...
    def apply_mutations(
        self,
        mutations_json: str,
        transaction_tag: str | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> SpannerCommitResult:
        """Commit a JSON-encoded list of Spanner mutations and return commit statistics."""
        ...
    def batch_write(
        self,
        mutation_groups_json: str,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[SpannerBatchWriteResult]:
        """Commit independent mutation groups via `BatchWrite` and return per-group statuses."""
        ...
    def update_ddl(
        self,
        statements: Sequence[str],
    ) -> str:
        """Execute one or more schema DDL statements via `UpdateDatabaseDdl`."""
        ...
    def partitioned_query(
        self,
        sql: str,
        params_json: str | None = None,
        max_partitions: int = 4,
        data_boost_enabled: bool = False,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> SpannerQueryResult:
        """Execute a parallel `PartitionQuery` read with optional Spanner Data Boost."""
        ...
    def begin_read_write_transaction(
        self,
        transaction_tag: str | None = None,
        previous_transaction_id: bytes | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> SyncSpannerReadWriteTransaction:
        """Open a stateful synchronous interactive ReadWrite transaction handle."""
        ...
    def cursor(
        self,
        *,
        staleness_mode: str = "strong",
        staleness_value: int = 0,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        param_encoder: Callable[[Any], str | None] | None = None,
        lazy_begin: bool = False,
    ) -> SyncSpannerCursor:
        """Create a synchronous PEP 249 cursor backed by this client."""
        ...
    def close(self) -> None:
        """Close the client handle."""
        ...

@final
class AsyncSpannerClient:
    """Asynchronous Google Cloud Spanner client built on the official Rust client.

    ``query`` and ``partitioned_query`` return ``columns``, ``schema``, ``rows``,
    ``row_count_exact``, and ``row_count_lower_bound``. ``ca_pem`` is accepted
    only for non-googleapis ``https://`` endpoints (Spanner Omni or private test
    servers).
    """

    def __new__(
        cls,
        database: str,
        database_role: str | None = None,
        endpoint: str | None = None,
        dialect: str = "googlesql",
        pool_size: int = 4,
        route_to_leader_enabled: bool = True,
        max_commit_delay_ms: int | None = None,
        auth_options: dict[str, str] | None = None,
        admin_endpoint: str | None = None,
        ca_pem: bytes | None = None,
    ) -> AsyncSpannerClient:
        """Initialize an asynchronous Cloud Spanner client handle."""
        ...
    @property
    def database(self) -> str:
        """Return the full `projects/{p}/instances/{i}/databases/{d}` resource URI."""
        ...
    @property
    def database_role(self) -> str | None:
        """Return the configured fine-grained access control database role, if any."""
        ...
    @property
    def endpoint(self) -> str:
        """Return the effective Cloud Spanner Data API gRPC endpoint."""
        ...
    @property
    def admin_endpoint(self) -> str | None:
        """Return the configured Cloud Spanner Database Admin REST endpoint, if any."""
        ...
    @property
    def dialect(self) -> str:
        """Return the SQL dialect (`googlesql` or `spangres`)."""
        ...
    @property
    def pool_size(self) -> int:
        """Return the configured gRPC channel pool size."""
        ...
    @property
    def route_to_leader_enabled(self) -> bool:
        """Return whether leader-aware routing headers are enabled."""
        ...
    @property
    def max_commit_delay_ms(self) -> int | None:
        """Return the configured maximum commit batching delay in milliseconds, if any."""
        ...
    def query(
        self,
        sql: str,
        params_json: str | None = None,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> Awaitable[SpannerQueryResult]:
        """Execute a streaming SQL read asynchronously on the shared Tokio runtime."""
        ...
    def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> Awaitable[int]:
        """Execute a transactional DML statement asynchronously."""
        ...
    def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> Awaitable[list[int]]:
        """Execute a batch of DML statements asynchronously."""
        ...
    def execute_partitioned_dml(
        self,
        sql: str,
        params_json: str | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> Awaitable[int]:
        """Execute a Partitioned DML statement asynchronously."""
        ...
    def apply_mutations(
        self,
        mutations_json: str,
        transaction_tag: str | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> Awaitable[SpannerCommitResult]:
        """Commit a JSON-encoded list of Spanner mutations asynchronously."""
        ...
    def batch_write(
        self,
        mutation_groups_json: str,
        exclude_txn_from_change_streams: bool = False,
    ) -> Awaitable[list[SpannerBatchWriteResult]]:
        """Commit independent mutation groups asynchronously via `BatchWrite`."""
        ...
    def update_ddl(
        self,
        statements: Sequence[str],
    ) -> Awaitable[str]:
        """Execute one or more schema DDL statements asynchronously via `UpdateDatabaseDdl`."""
        ...
    def partitioned_query(
        self,
        sql: str,
        params_json: str | None = None,
        max_partitions: int = 4,
        data_boost_enabled: bool = False,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> Awaitable[SpannerQueryResult]:
        """Execute a parallel `PartitionQuery` read asynchronously with optional Data Boost."""
        ...
    def begin_read_write_transaction(
        self,
        transaction_tag: str | None = None,
        previous_transaction_id: bytes | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> Awaitable[AsyncSpannerReadWriteTransaction]:
        """Open a stateful asynchronous interactive ReadWrite transaction handle."""
        ...
    def cursor(
        self,
        *,
        staleness_mode: str = "strong",
        staleness_value: int = 0,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        param_encoder: Callable[[Any], str | None] | None = None,
        lazy_begin: bool = False,
    ) -> AsyncSpannerCursor:
        """Create an asynchronous PEP 249 cursor backed by this client."""
        ...
    def close(self) -> None:
        """Close the client handle."""
        ...
