"""Type stubs for the vertebra._postgres native extension module."""

from collections.abc import Awaitable, Buffer, Mapping, Sequence
from types import TracebackType
from typing import final

from vertebra._vertebra import ArrowCursor as _NativeArrowCursor
from vertebra.db.adapters.postgres.types import (
    PgParameter,
    PostgresArrowResult,
    PostgresPipelineOperation,
    PostgresPipelineOutcome,
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

    def cancel(self) -> None:
        """Trigger an out-of-band generation-fenced protocol CancelRequest for any active operation."""
        ...

    def query(
        self,
        sql: str,
        parameters: Sequence[PostgresValue | PgParameter] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> PostgresQueryResult:
        """Execute a single-statement PostgreSQL Protocol 3.0 extended query.

        When timeout is provided, it specifies a single positive I/O budget in seconds
        started at native operation reservation covering parameter discovery, wire frames,
        and active metadata enrichment without preempting CPU decode or Python projection.
        """
        ...

    def query_multi(
        self,
        sql: str,
        *,
        timeout: float | None = None,
    ) -> tuple[PostgresQueryResult, ...]:
        """Execute a multi-statement PostgreSQL Protocol 3.0 simple query ('Q').

        When timeout is provided, one non-renewing I/O budget in seconds started at
        native operation reservation covers the full multi-statement command without
        preempting CPU decode or Python projection.
        """
        ...

    def open_portal(
        self,
        sql: str,
        parameters: Sequence[PostgresValue | PgParameter] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        batch_size: int = 65536,
    ) -> SyncPostgresPortal:
        """Open a session-pinned row-limited PostgreSQL portal over sql."""
        ...

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[PostgresValue | PgParameter] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
        threshold_rows: int = 256,
        max_result_bytes: int = 268435456,
    ) -> PostgresArrowResult:
        """Execute sql directly into an inline or VTBARROW shared-memory PostgresArrowResult."""
        ...

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[PostgresValue | PgParameter] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        batch_size: int = 65536,
        columns: Sequence[str] | None = None,
    ) -> SyncPostgresArrowStream:
        """Open a session-pinned portal stream yielding VTBARROW shared-memory ArrowCursor batches."""
        ...

    def copy_to(self, sql: str) -> SyncPostgresCopyOut:
        """Open an exclusive binary COPY ... TO STDOUT streaming iterator."""
        ...

    def copy_from(self, sql: str) -> SyncPostgresCopyIn:
        """Open an exclusive binary COPY ... FROM STDIN write stream."""
        ...

    def copy_from_arrow(
        self,
        table_name: str,
        first_batch_ipc: Buffer,
        *,
        columns: Sequence[str] | None = None,
        schema_name: str | None = None,
    ) -> SyncPostgresCopyIn:
        """Validate schema against pg_catalog and open a binary COPY ... FROM STDIN stream seeded with first_batch_ipc."""
        ...

    def pipeline_begin(self) -> int:
        """Open a retained pipeline transaction or savepoint scope and return its scope_id."""
        ...

    def pipeline_results(
        self,
        scope_id: int,
        operations: Sequence[PostgresPipelineOperation],
    ) -> tuple[PostgresPipelineOutcome, ...]:
        """Submit operations as one single-Sync extended batch inside scope_id while retaining the open scope."""
        ...

    def pipeline_finish(self, scope_id: int, success: bool) -> None:
        """Commit/release (success=True) or roll back (success=False or persistent failure) scope_id."""
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

    def cancel(self) -> Awaitable[None]:
        """Trigger an out-of-band generation-fenced protocol CancelRequest asynchronously."""
        ...

    def query(
        self,
        sql: str,
        parameters: Sequence[PostgresValue | PgParameter] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Awaitable[PostgresQueryResult]:
        """Execute a single-statement PostgreSQL Protocol 3.0 extended query asynchronously.

        When timeout is provided, it specifies a single positive I/O budget in seconds
        started at native operation reservation before async worker scheduling without
        preempting CPU decode or Python projection.
        """
        ...

    def query_multi(
        self,
        sql: str,
        *,
        timeout: float | None = None,
    ) -> Awaitable[tuple[PostgresQueryResult, ...]]:
        """Execute a multi-statement PostgreSQL Protocol 3.0 simple query ('Q') asynchronously.

        When timeout is provided, one non-renewing I/O budget in seconds started at
        native operation reservation covers the full multi-statement command without
        preempting CPU decode or Python projection.
        """
        ...

    def open_portal(
        self,
        sql: str,
        parameters: Sequence[PostgresValue | PgParameter] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        batch_size: int = 65536,
    ) -> Awaitable[AsyncPostgresPortal]:
        """Open a session-pinned row-limited PostgreSQL portal asynchronously over sql."""
        ...

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[PostgresValue | PgParameter] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
        threshold_rows: int = 256,
        max_result_bytes: int = 268435456,
    ) -> Awaitable[PostgresArrowResult]:
        """Execute a single-statement query directly into inline or VTBARROW SHM Arrow IPC asynchronously."""
        ...

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[PostgresValue | PgParameter] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        batch_size: int = 65536,
        columns: Sequence[str] | None = None,
    ) -> AsyncPostgresArrowStream:
        """Validate options synchronously and return a cancellation-safe AsyncPostgresArrowStream."""
        ...

    def copy_to(self, sql: str) -> Awaitable[AsyncPostgresCopyOut]:
        """Open an exclusive binary COPY ... TO STDOUT streaming iterator asynchronously."""
        ...

    def copy_from(self, sql: str) -> Awaitable[AsyncPostgresCopyIn]:
        """Open an exclusive binary COPY ... FROM STDIN write stream asynchronously."""
        ...

    def copy_from_arrow(
        self,
        table_name: str,
        first_batch_ipc: Buffer,
        *,
        columns: Sequence[str] | None = None,
        schema_name: str | None = None,
    ) -> Awaitable[AsyncPostgresCopyIn]:
        """Validate schema against pg_catalog and open an async binary COPY ... FROM STDIN stream seeded with first_batch_ipc."""
        ...

    def pipeline_begin(self) -> Awaitable[int]:
        """Open a retained pipeline transaction or savepoint scope asynchronously and return its scope_id."""
        ...

    def pipeline_results(
        self,
        scope_id: int,
        operations: Sequence[PostgresPipelineOperation],
    ) -> Awaitable[tuple[PostgresPipelineOutcome, ...]]:
        """Submit operations asynchronously as one single-Sync extended batch inside scope_id."""
        ...

    def pipeline_finish(self, scope_id: int, success: bool) -> Awaitable[None]:
        """Commit/release (success=True) or roll back (success=False or persistent failure) scope_id asynchronously."""
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

@final
class SyncPostgresPool:
    """Synchronous physical PostgreSQL Protocol 3.0 connection pool."""

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
        pooler_mode: str = "direct",
        protocol_level_pooler_cache: bool = False,
        statement_cache_size: int = 256,
        min_size: int = 1,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
        reset_timeout: float = 10.0,
        close_timeout: float = 30.0,
    ) -> SyncPostgresPool:
        """Validate connection and pool sizing options without network I/O."""
        ...

    def acquire(self) -> SyncPostgresClient:
        """Acquire a concrete pooled physical connection lease."""
        ...

    def close(self) -> None:
        """Drain and close the connection pool under close_timeout."""
        ...

    def validate_configuration(
        self,
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
        pooler_mode: str = "direct",
        protocol_level_pooler_cache: bool = False,
        statement_cache_size: int = 256,
        min_size: int = 1,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
        reset_timeout: float = 10.0,
        close_timeout: float = 30.0,
    ) -> None:
        """Validate candidate connection and preparation policy against this pool without I/O."""
        ...

    @property
    def closed(self) -> bool:
        """Return True if the pool has been closed or is draining."""
        ...

@final
class AsyncPostgresPool:
    """Asynchronous physical PostgreSQL Protocol 3.0 connection pool."""

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
        pooler_mode: str = "direct",
        protocol_level_pooler_cache: bool = False,
        statement_cache_size: int = 256,
        min_size: int = 1,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
        reset_timeout: float = 10.0,
        close_timeout: float = 30.0,
    ) -> AsyncPostgresPool:
        """Validate connection and pool sizing options without network I/O."""
        ...

    def acquire(self) -> Awaitable[AsyncPostgresClient]:
        """Acquire a concrete pooled physical connection lease asynchronously."""
        ...

    def close(self) -> Awaitable[None]:
        """Drain and close the connection pool asynchronously under close_timeout."""
        ...

    def validate_configuration(
        self,
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
        pooler_mode: str = "direct",
        protocol_level_pooler_cache: bool = False,
        statement_cache_size: int = 256,
        min_size: int = 1,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
        reset_timeout: float = 10.0,
        close_timeout: float = 30.0,
    ) -> None:
        """Validate candidate connection and preparation policy against this pool without I/O."""
        ...

    @property
    def closed(self) -> bool:
        """Return True if the pool has been closed or is draining."""
        ...

@final
class SyncPostgresPortal:
    """Session-pinned synchronous PostgreSQL Protocol 3.0 portal."""

    def fetch(self, max_rows: int) -> PostgresQueryResult:
        """Fetch up to max_rows rows (>= 1) from the suspended portal."""
        ...

    def close(self) -> None:
        """Close the portal idempotently and finish its pinned transaction scope."""
        ...

    @property
    def exhausted(self) -> bool:
        """Return True once the server portal has reached completion."""
        ...

    @property
    def closed(self) -> bool:
        """Return True once the portal has been closed or exhausted."""
        ...

@final
class AsyncPostgresPortal:
    """Session-pinned asynchronous PostgreSQL Protocol 3.0 portal."""

    def fetch(self, max_rows: int) -> Awaitable[PostgresQueryResult]:
        """Fetch up to max_rows rows (>= 1) asynchronously from the suspended portal."""
        ...

    def close(self) -> Awaitable[None]:
        """Close the portal idempotently and finish its pinned transaction scope asynchronously."""
        ...

    @property
    def exhausted(self) -> bool:
        """Return True once the server portal has reached completion."""
        ...

    @property
    def closed(self) -> bool:
        """Return True once the portal has been closed or exhausted."""
        ...

@final
class SyncPostgresArrowStream:
    """Session-pinned synchronous VTBARROW shared-memory Arrow IPC batch iterator."""

    def __iter__(self) -> SyncPostgresArrowStream:
        """Return self as a synchronous ArrowCursor batch iterator."""
        ...

    def __next__(self) -> _NativeArrowCursor:
        """Advance the server portal and return the next native SHM ArrowCursor batch."""
        ...

    def close(self) -> None:
        """Close the pinned Arrow portal stream idempotently."""
        ...

    def __enter__(self) -> SyncPostgresArrowStream:
        """Enter a with block returning this SyncPostgresArrowStream."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool:
        """Close the stream on context exit without suppressing exceptions."""
        ...

@final
class AsyncPostgresArrowStream:
    """Cancellation-safe asynchronous VTBARROW shared-memory Arrow IPC batch iterator."""

    def __aiter__(self) -> AsyncPostgresArrowStream:
        """Return self as an asynchronous ArrowCursor batch iterator."""
        ...

    def __anext__(self) -> Awaitable[_NativeArrowCursor]:
        """Advance the server portal off the event loop and resolve to the next SHM ArrowCursor batch."""
        ...

    def aclose(self) -> Awaitable[None]:
        """Idempotently close or cancel and retire the pinned Arrow portal stream."""
        ...

    def __aenter__(self) -> Awaitable[AsyncPostgresArrowStream]:
        """Enter an async with block resolving to this AsyncPostgresArrowStream."""
        ...

    def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> Awaitable[None]:
        """Close the stream on async context exit without suppressing exceptions."""
        ...

@final
class SyncPostgresCopyOut:
    """Exclusive synchronous binary COPY ... TO STDOUT row iterator."""

    def __iter__(self) -> SyncPostgresCopyOut:
        """Return self as a synchronous row iterator."""
        ...

    def __next__(self) -> tuple[bytes | None, ...]:
        """Yield the next decoded binary row tuple or raise StopIteration."""
        ...

    def close(self) -> None:
        """Close an unfinished COPY TO stream idempotently via CancelRequest drain."""
        ...

    def __enter__(self) -> SyncPostgresCopyOut:
        """Enter a with block returning this SyncPostgresCopyOut."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool:
        """Close the stream on context exit without suppressing exceptions."""
        ...

@final
class SyncPostgresCopyIn:
    """Exclusive synchronous binary COPY ... FROM STDIN write stream."""

    def write(
        self,
        rows: Sequence[Sequence[PostgresValue]],
        column_oids: Sequence[int],
    ) -> None:
        """Encode and flush binary COPY rows against explicit PostgreSQL column OIDs."""
        ...

    def write_arrow_ipc(self, batch_ipc: Buffer) -> None:
        """Encode and flush a bounded (<= 16 MiB) Arrow IPC stream chunk."""
        ...

    def finish(self) -> int:
        """Emit the binary trailer and CopyDone and return the inserted row count."""
        ...

    def close(self) -> None:
        """Abort an unfinished COPY FROM stream via CopyFail or no-op once closed."""
        ...

    def __enter__(self) -> SyncPostgresCopyIn:
        """Enter a with block returning this SyncPostgresCopyIn."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool:
        """Abort via close() if unfinished or on exception without suppressing exceptions."""
        ...

@final
class AsyncPostgresCopyOut:
    """Exclusive asynchronous binary COPY ... TO STDOUT row iterator."""

    def __aiter__(self) -> AsyncPostgresCopyOut:
        """Return self as an asynchronous row iterator."""
        ...

    def __anext__(self) -> Awaitable[tuple[bytes | None, ...]]:
        """Resolve to the next decoded binary row tuple or raise StopAsyncIteration."""
        ...

    def aclose(self) -> Awaitable[None]:
        """Asynchronously close an unfinished COPY TO stream idempotently."""
        ...

    def __aenter__(self) -> Awaitable[AsyncPostgresCopyOut]:
        """Enter an async with block resolving to this AsyncPostgresCopyOut."""
        ...

    def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> Awaitable[bool]:
        """Asynchronously close the stream on context exit without suppressing exceptions."""
        ...

@final
class AsyncPostgresCopyIn:
    """Exclusive asynchronous binary COPY ... FROM STDIN write stream."""

    def write(
        self,
        rows: Sequence[Sequence[PostgresValue]],
        column_oids: Sequence[int],
    ) -> Awaitable[None]:
        """Asynchronously encode and flush binary COPY rows against PostgreSQL column OIDs."""
        ...

    def write_arrow_ipc(self, batch_ipc: Buffer) -> Awaitable[None]:
        """Asynchronously encode and flush a bounded (<= 16 MiB) Arrow IPC stream chunk."""
        ...

    def finish(self) -> Awaitable[int]:
        """Asynchronously emit the binary trailer and CopyDone and resolve the row count."""
        ...

    def aclose(self) -> Awaitable[None]:
        """Asynchronously abort an unfinished COPY FROM stream via CopyFail or no-op once closed."""
        ...

    def __aenter__(self) -> Awaitable[AsyncPostgresCopyIn]:
        """Enter an async with block resolving to this AsyncPostgresCopyIn."""
        ...

    def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> Awaitable[bool]:
        """Abort via aclose() if unfinished or on exception without suppressing exceptions."""
        ...

__all__ = [
    "FREE_THREADED",
    "GIL_USED",
    "AsyncPostgresArrowStream",
    "AsyncPostgresClient",
    "AsyncPostgresCopyIn",
    "AsyncPostgresCopyOut",
    "AsyncPostgresPool",
    "AsyncPostgresPortal",
    "SyncPostgresArrowStream",
    "SyncPostgresClient",
    "SyncPostgresCopyIn",
    "SyncPostgresCopyOut",
    "SyncPostgresPool",
    "SyncPostgresPortal",
    "__version__",
]
