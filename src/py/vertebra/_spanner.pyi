"""Type stubs for the vertebra._spanner native extension module."""

from collections.abc import Awaitable, Sequence
from typing import Any, final

__version__: str
FREE_THREADED: bool
GIL_USED: bool

__all__ = [
    "FREE_THREADED",
    "GIL_USED",
    "AsyncSpannerClient",
    "SyncSpannerClient",
    "__version__",
]

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
    ) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
        """Commit a JSON-encoded list of Spanner mutations and return commit statistics."""
        ...
    def batch_write(
        self,
        mutation_groups_json: str,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[dict[str, Any]]:
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
    ) -> dict[str, Any]:
        """Execute a parallel `PartitionQuery` read with optional Spanner Data Boost."""
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
    ) -> Awaitable[dict[str, Any]]:
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
    ) -> Awaitable[dict[str, Any]]:
        """Commit a JSON-encoded list of Spanner mutations asynchronously."""
        ...
    def batch_write(
        self,
        mutation_groups_json: str,
        exclude_txn_from_change_streams: bool = False,
    ) -> Awaitable[list[dict[str, Any]]]:
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
    ) -> Awaitable[dict[str, Any]]:
        """Execute a parallel `PartitionQuery` read asynchronously with optional Data Boost."""
        ...
    def close(self) -> None:
        """Close the client handle."""
        ...
