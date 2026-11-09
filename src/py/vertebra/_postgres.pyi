"""Type stubs for the vertebra._postgres native extension module."""

from collections.abc import Awaitable, Mapping, Sequence
from typing import final

from vertebra.db.adapters.postgres.types import (
    PgParameter,
    PostgresQueryResult,
    PostgresValue,
)

__version__: str
FREE_THREADED: bool
GIL_USED: bool

@final
class SyncPostgresClient:
    """Synchronous physical PostgreSQL Protocol 3.0 client."""

    def __new__(
        cls,
        *,
        user: str,
        database: str,
        host: str = "localhost",
        port: int = 5432,
        password: str | None = None,
        unix_socket: str | None = None,
        sslmode: str = "verify-full",
        ca_pem: bytes | None = None,
        client_cert_pem: bytes | None = None,
        client_key_pem: bytes | None = None,
        server_name: str | None = None,
        channel_binding: str = "prefer",
        connect_timeout: float = 10.0,
        application_name: str = "vertebra",
        session_gucs: Mapping[str, str] | None = None,
        keepalives: bool = True,
        keepalives_idle: int | None = None,
        keepalives_interval: int | None = None,
        keepalives_count: int | None = None,
        cloud_sql_instance: str | None = None,
        cloud_sql_ip_type: str = "PRIVATE",
        alloydb_instance_uri: str | None = None,
        alloydb_ip_type: str = "PRIVATE",
        enable_iam_auth: bool = False,
        cloud_auth_options: Mapping[str, str] | None = None,
        iam_auth_options: Mapping[str, str] | None = None,
    ) -> SyncPostgresClient:
        """Validate and construct a disconnected synchronous PostgreSQL client without network I/O."""
        ...

    def connect(self) -> None:
        """Establish a physical PostgreSQL session under connect_timeout."""
        ...

    def close(self) -> None:
        """Close the physical PostgreSQL session idempotently."""
        ...

    def query(
        self,
        sql: str,
        parameters: Sequence[PostgresValue | PgParameter] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
    ) -> PostgresQueryResult:
        """Execute a single-statement PostgreSQL Protocol 3.0 extended query."""
        ...

    def query_multi(self, sql: str) -> tuple[PostgresQueryResult, ...]:
        """Execute a multi-statement PostgreSQL Protocol 3.0 simple query ('Q')."""
        ...

    def begin(self) -> None:
        """Start an explicit transaction ('BEGIN')."""
        ...

    def commit(self) -> None:
        """Commit the current transaction ('COMMIT')."""
        ...

    def rollback(self) -> None:
        """Roll back the current transaction ('ROLLBACK')."""
        ...

    def savepoint(self, token: int) -> None:
        """Create a numeric savepoint ('SAVEPOINT vtb_sp_<token>')."""
        ...

    def release_savepoint(self, token: int) -> None:
        """Release an active numeric savepoint ('RELEASE SAVEPOINT vtb_sp_<token>')."""
        ...

    def rollback_to_savepoint(self, token: int) -> None:
        """Roll back to an active numeric savepoint ('ROLLBACK TO SAVEPOINT vtb_sp_<token>')."""
        ...

    @property
    def closed(self) -> bool:
        """Return True unless an authenticated session is currently active."""
        ...

    @property
    def parameters(self) -> dict[str, str]:
        """Return a copied dictionary of server ParameterStatus values."""
        ...

@final
class AsyncPostgresClient:
    """Asynchronous physical PostgreSQL Protocol 3.0 client."""

    def __new__(
        cls,
        *,
        user: str,
        database: str,
        host: str = "localhost",
        port: int = 5432,
        password: str | None = None,
        unix_socket: str | None = None,
        sslmode: str = "verify-full",
        ca_pem: bytes | None = None,
        client_cert_pem: bytes | None = None,
        client_key_pem: bytes | None = None,
        server_name: str | None = None,
        channel_binding: str = "prefer",
        connect_timeout: float = 10.0,
        application_name: str = "vertebra",
        session_gucs: Mapping[str, str] | None = None,
        keepalives: bool = True,
        keepalives_idle: int | None = None,
        keepalives_interval: int | None = None,
        keepalives_count: int | None = None,
        cloud_sql_instance: str | None = None,
        cloud_sql_ip_type: str = "PRIVATE",
        alloydb_instance_uri: str | None = None,
        alloydb_ip_type: str = "PRIVATE",
        enable_iam_auth: bool = False,
        cloud_auth_options: Mapping[str, str] | None = None,
        iam_auth_options: Mapping[str, str] | None = None,
    ) -> AsyncPostgresClient:
        """Validate and construct a disconnected asynchronous PostgreSQL client without network I/O."""
        ...

    def connect(self) -> Awaitable[None]:
        """Establish a physical PostgreSQL session asynchronously under connect_timeout."""
        ...

    def close(self) -> Awaitable[None]:
        """Mark the session closed and schedule asynchronous Terminate shutdown."""
        ...

    def query(
        self,
        sql: str,
        parameters: Sequence[PostgresValue | PgParameter] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
    ) -> Awaitable[PostgresQueryResult]:
        """Execute a single-statement PostgreSQL Protocol 3.0 extended query asynchronously."""
        ...

    def query_multi(self, sql: str) -> Awaitable[tuple[PostgresQueryResult, ...]]:
        """Execute a multi-statement PostgreSQL Protocol 3.0 simple query ('Q') asynchronously."""
        ...

    def begin(self) -> Awaitable[None]:
        """Start an explicit transaction ('BEGIN') asynchronously."""
        ...

    def commit(self) -> Awaitable[None]:
        """Commit the current transaction ('COMMIT') asynchronously."""
        ...

    def rollback(self) -> Awaitable[None]:
        """Roll back the current transaction ('ROLLBACK') asynchronously."""
        ...

    def savepoint(self, token: int) -> Awaitable[None]:
        """Create a numeric savepoint ('SAVEPOINT vtb_sp_<token>') asynchronously."""
        ...

    def release_savepoint(self, token: int) -> Awaitable[None]:
        """Release an active numeric savepoint ('RELEASE SAVEPOINT vtb_sp_<token>') asynchronously."""
        ...

    def rollback_to_savepoint(self, token: int) -> Awaitable[None]:
        """Roll back to an active numeric savepoint ('ROLLBACK TO SAVEPOINT vtb_sp_<token>') asynchronously."""
        ...

    @property
    def closed(self) -> bool:
        """Return True unless an authenticated session is currently active."""
        ...

    @property
    def parameters(self) -> dict[str, str]:
        """Return a copied dictionary of server ParameterStatus values."""
        ...

__all__ = [
    "FREE_THREADED",
    "GIL_USED",
    "AsyncPostgresClient",
    "SyncPostgresClient",
    "__version__",
]
