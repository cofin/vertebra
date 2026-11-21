"""Type stubs for the vertebra._bigquery native extension module."""

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, NotRequired, Self, TypedDict, final, type_check_only

from vertebra._vertebra import ArrowCursor

@type_check_only
class BigQueryFieldSchema(TypedDict):
    """BigQuery result schema field descriptor."""

    name: str
    """Field name."""
    type: str
    """BigQuery legacy or standard SQL type name."""
    mode: str
    """Column mode (``NULLABLE``, ``REQUIRED``, or ``REPEATED``)."""
    fields: NotRequired[list[BigQueryFieldSchema]]
    """Nested STRUCT or RECORD child field schemas."""

@type_check_only
class BigQueryQueryResult(TypedDict):
    """BigQuery synchronous or asynchronous query execution payload."""

    columns: list[str]
    """Ordered output column names."""
    schema: list[BigQueryFieldSchema]
    """Field schema metadata for output columns."""
    rows: list[list[Any]]
    """Decoded tabular result rows."""
    total_rows: int
    """Total rows matched by the query job."""
    total_bytes_processed: int
    """Total bytes processed across queried tables and partitions."""
    total_bytes_billed: int
    """Total bytes billed for query execution."""
    slot_ms: int
    """Cumulative slot milliseconds consumed by the query job."""
    cache_hit: bool
    """Whether results were served from BigQuery query cache."""
    job_id: str | None
    """Assigned BigQuery job identifier, if created."""
    session_id: str | None
    """Active BigQuery session identifier, if any."""
    num_dml_affected_rows: int | None
    """Number of rows inserted, updated, or deleted by a DML statement."""

@type_check_only
class BigQueryDryRunResult(TypedDict):
    """BigQuery query dry-run cost and schema estimation payload."""

    total_bytes_processed: int
    """Estimated bytes that execution would scan."""
    total_bytes_billed: int
    """Estimated billable bytes for the query."""
    cache_hit: bool
    """Whether the dry-run metadata matched a cached result."""
    referenced_tables: list[str]
    """Fully-qualified table references inspected by the dry run."""
    schema: list[BigQueryFieldSchema]
    """Inferred output schema for the validated query."""

@type_check_only
class BigQueryAppendResult(TypedDict):
    """BigQuery Storage Write API append confirmation payload."""

    appended_rows: int
    """Number of rows appended to the write stream."""
    write_stream: str
    """Target BigQuery Storage Write API stream resource name."""

__version__: str
FREE_THREADED: bool
GIL_USED: bool

__all__ = [
    "FREE_THREADED",
    "GIL_USED",
    "AsyncBigQueryClient",
    "AsyncBigQueryCursor",
    "SyncBigQueryClient",
    "SyncBigQueryCursor",
    "__version__",
]

@final
class SyncBigQueryClient:
    """Synchronous pure-Rust Google Cloud BigQuery REST v2 and Storage Read/Write client.

    ``ca_pem`` applies to the REST API client; the Storage Read and Write API
    clients use the system trust store and ignore it.
    """

    def __new__(
        cls,
        project_id: str,
        dataset_id: str | None = None,
        location: str | None = None,
        rest_endpoint: str | None = None,
        storage_endpoint: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: str | None = None,
        labels: dict[str, str] | None = None,
        auth_options: dict[str, str] | None = None,
        pool_size: int = 4,
        ca_pem: bytes | None = None,
    ) -> SyncBigQueryClient:
        """Initialize a synchronous BigQuery client handle."""
        ...
    @property
    def project_id(self) -> str:
        """Return the configured Google Cloud project ID."""
        ...
    @property
    def dataset_id(self) -> str | None:
        """Return the default BigQuery dataset ID, if configured."""
        ...
    @property
    def location(self) -> str | None:
        """Return the default BigQuery job location, if configured."""
        ...
    @property
    def rest_endpoint(self) -> str:
        """Return the effective BigQuery REST v2 endpoint URL."""
        ...
    @property
    def storage_endpoint(self) -> str:
        """Return the effective BigQuery Storage API gRPC endpoint URL."""
        ...
    @property
    def maximum_bytes_billed(self) -> int | None:
        """Return the per-query byte billing cap, if configured."""
        ...
    @property
    def job_timeout_ms(self) -> int | None:
        """Return the default query job timeout in milliseconds, if configured."""
        ...
    @property
    def priority(self) -> str | None:
        """Return the default query priority (`INTERACTIVE` or `BATCH`), if configured."""
        ...
    @property
    def pool_size(self) -> int:
        """Return the configured channel pool size."""
        ...
    def query(
        self,
        sql: str,
        params_json: str | None = None,
        session_id: str | None = None,
        create_session: bool = False,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> BigQueryQueryResult:
        """Execute a GoogleSQL query via `jobs.query` and paginate through `jobs.getQueryResults`."""
        ...
    def dry_run(
        self,
        sql: str,
        params_json: str | None = None,
    ) -> BigQueryDryRunResult:
        """Validate `sql` via `jobs.insert(dryRun=True)` and return byte and schema estimates."""
        ...
    def read_table_arrow(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
        max_streams: int = 4,
        compression: str = "LZ4_FRAME",
    ) -> ArrowCursor:
        """Stream a BigQuery table via the Storage Read API into a zero-copy `ArrowCursor`."""
        ...
    def append_arrow(
        self,
        parent_table: str,
        arrow_ipc_bytes: bytes,
        mode: str = "_default",
    ) -> BigQueryAppendResult:
        """Append an Arrow IPC stream to `parent_table` via the Storage Write API."""
        ...
    def close(self) -> None:
        """Close the client handle."""
        ...
    def cursor(
        self,
        *,
        session_id: str | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: str | None = None,
        labels: Mapping[str, str] | None = None,
        param_encoder: Callable[[str, Any], tuple[str, str | None]] | None = None,
        lazy_begin: bool = False,
    ) -> SyncBigQueryCursor:
        """Create a synchronous PEP 249 cursor backed by this BigQuery client."""
        ...

@final
class SyncBigQueryCursor:
    """Synchronous PEP 249-shaped cursor over a native BigQuery client."""

    arraysize: int

    @property
    def description(self) -> tuple[tuple[str, str, None, None, None, None, bool | None], ...] | None:
        """Return the column descriptions of the current result set, or `None`."""
        ...
    @property
    def rowcount(self) -> int:
        """Return the row or DML count of the last statement, or `-1`."""
        ...
    @property
    def lastrowid(self) -> int | None:
        """Return `None` (BigQuery does not report last insert rowids)."""
        ...
    @property
    def query_metadata(self) -> dict[str, Any] | None:
        """Return job metadata for the last executed query, or `None` before any."""
        ...
    def execute(
        self,
        sql: str,
        params: str | Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Self:
        """Run one query job and buffer its rows, description, and metadata."""
        ...
    def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[str | Sequence[Any] | Mapping[str, Any]],
    ) -> Self:
        """Run one query job per parameter set in order and sum DML row counts."""
        ...
    def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next buffered row, or `None` when exhausted."""
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
    def __enter__(self) -> Self:
        """Enter a context manager block returning this cursor."""
        ...
    def __exit__(
        self,
        exc_type: object = None,
        exc_value: object = None,
        traceback: object = None,
    ) -> bool:
        """Close the cursor on context manager exit."""
        ...

@final
class AsyncBigQueryClient:
    """Asynchronous pure-Rust Google Cloud BigQuery REST v2 and Storage Read/Write client.

    ``ca_pem`` applies to the REST API client; the Storage Read and Write API
    clients use the system trust store and ignore it.
    """

    def __new__(
        cls,
        project_id: str,
        dataset_id: str | None = None,
        location: str | None = None,
        rest_endpoint: str | None = None,
        storage_endpoint: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: str | None = None,
        labels: dict[str, str] | None = None,
        auth_options: dict[str, str] | None = None,
        pool_size: int = 4,
        ca_pem: bytes | None = None,
    ) -> AsyncBigQueryClient:
        """Initialize an asynchronous BigQuery client handle."""
        ...
    @property
    def project_id(self) -> str:
        """Return the configured Google Cloud project ID."""
        ...
    @property
    def dataset_id(self) -> str | None:
        """Return the default BigQuery dataset ID, if configured."""
        ...
    @property
    def location(self) -> str | None:
        """Return the default BigQuery job location, if configured."""
        ...
    @property
    def rest_endpoint(self) -> str:
        """Return the effective BigQuery REST v2 endpoint URL."""
        ...
    @property
    def storage_endpoint(self) -> str:
        """Return the effective BigQuery Storage API gRPC endpoint URL."""
        ...
    @property
    def maximum_bytes_billed(self) -> int | None:
        """Return the per-query byte billing cap, if configured."""
        ...
    @property
    def job_timeout_ms(self) -> int | None:
        """Return the default query job timeout in milliseconds, if configured."""
        ...
    @property
    def priority(self) -> str | None:
        """Return the default query priority (`INTERACTIVE` or `BATCH`), if configured."""
        ...
    @property
    def pool_size(self) -> int:
        """Return the configured channel pool size."""
        ...
    def query(
        self,
        sql: str,
        params_json: str | None = None,
        session_id: str | None = None,
        create_session: bool = False,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> Awaitable[BigQueryQueryResult]:
        """Execute a GoogleSQL query asynchronously on the shared Tokio runtime."""
        ...
    def dry_run(
        self,
        sql: str,
        params_json: str | None = None,
    ) -> Awaitable[BigQueryDryRunResult]:
        """Validate `sql` asynchronously via `jobs.insert(dryRun=True)`."""
        ...
    def read_table_arrow(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
        max_streams: int = 4,
        compression: str = "LZ4_FRAME",
    ) -> Awaitable[ArrowCursor]:
        """Stream a BigQuery table asynchronously into a zero-copy `ArrowCursor`."""
        ...
    def append_arrow(
        self,
        parent_table: str,
        arrow_ipc_bytes: bytes,
        mode: str = "_default",
    ) -> Awaitable[BigQueryAppendResult]:
        """Append an Arrow IPC stream asynchronously via the Storage Write API."""
        ...
    def close(self) -> None:
        """Close the client handle."""
        ...
    def cursor(
        self,
        *,
        session_id: str | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: str | None = None,
        labels: Mapping[str, str] | None = None,
        param_encoder: Callable[[str, Any], tuple[str, str | None]] | None = None,
        lazy_begin: bool = False,
    ) -> AsyncBigQueryCursor:
        """Create an asynchronous PEP 249 cursor backed by this BigQuery client."""
        ...

@final
class AsyncBigQueryCursor:
    """Asynchronous PEP 249-shaped cursor over a native BigQuery client."""

    arraysize: int

    @property
    def description(self) -> tuple[tuple[str, str, None, None, None, None, bool | None], ...] | None:
        """Return the column descriptions of the current result set, or `None`."""
        ...
    @property
    def rowcount(self) -> int:
        """Return the row or DML count of the last statement, or `-1`."""
        ...
    @property
    def lastrowid(self) -> int | None:
        """Return `None` (BigQuery does not report last insert rowids)."""
        ...
    @property
    def query_metadata(self) -> dict[str, Any] | None:
        """Return job metadata for the last executed query, or `None` before any."""
        ...
    def execute(
        self,
        sql: str,
        params: str | Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Awaitable[Self]:
        """Run one query job asynchronously and buffer its rows, description, and metadata."""
        ...
    def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[str | Sequence[Any] | Mapping[str, Any]],
    ) -> Awaitable[Self]:
        """Run one query job per parameter set in order asynchronously and sum DML row counts."""
        ...
    def fetchone(self) -> Awaitable[tuple[Any, ...] | None]:
        """Return the next buffered row, or `None` when exhausted."""
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
