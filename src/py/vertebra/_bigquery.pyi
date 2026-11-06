"""Type stubs for the vertebra._bigquery native extension module."""

from collections.abc import Awaitable, Sequence
from typing import Any, final

from vertebra._vertebra import ArrowCursor

__version__: str
FREE_THREADED: bool
GIL_USED: bool

__all__ = [
    "FREE_THREADED",
    "GIL_USED",
    "AsyncBigQueryClient",
    "SyncBigQueryClient",
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
    ) -> dict[str, Any]:
        """Execute a GoogleSQL query via `jobs.query` and paginate through `jobs.getQueryResults`."""
        ...
    def dry_run(
        self,
        sql: str,
        params_json: str | None = None,
    ) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
        """Append an Arrow IPC stream to `parent_table` via the Storage Write API."""
        ...
    def close(self) -> None:
        """Close the client handle."""
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
    ) -> Awaitable[dict[str, Any]]:
        """Execute a GoogleSQL query asynchronously on the shared Tokio runtime."""
        ...
    def dry_run(
        self,
        sql: str,
        params_json: str | None = None,
    ) -> Awaitable[dict[str, Any]]:
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
    ) -> Awaitable[dict[str, Any]]:
        """Append an Arrow IPC stream asynchronously via the Storage Write API."""
        ...
    def close(self) -> None:
        """Close the client handle."""
        ...
