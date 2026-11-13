"""Native synchronous PostgreSQL driver backed by ``vertebra._postgres``."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra._vertebra import InterfaceError, ProgrammingError
from vertebra.db.adapters.postgres.arrow import load_arrow_sync
from vertebra.db.adapters.postgres.binds import compile_postgres_binds
from vertebra.db.adapters.postgres.change_stream import (
    PostgresChangeConfig,
    PostgresChangeSource,
    PostgresCheckpointStore,
)
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.adapters.postgres.core import PostgreSQLCore
from vertebra.db.adapters.postgres.events import EventSessionRegistry
from vertebra.db.adapters.postgres.notify import PostgresNotificationSource, PostgresNotifyConfig
from vertebra.db.adapters.postgres.options import build_native_client_options
from vertebra.db.adapters.postgres.pipeline import SyncPostgresPipeline
from vertebra.db.adapters.postgres.types import (
    PostgresArrowResult,
    PostgresQueryResult,
    PostgresReplicationIdentity,
    _project_public_row,
)
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.db.driver._common import POSTGRES_DIALECT_TAG
from vertebra.db.driver._sync import SyncDatabaseDriver
from vertebra.db.rls import TenantScopeConfig
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from collections.abc import Buffer

    from vertebra._postgres import AsyncPostgresListener, AsyncPostgresReplication
    from vertebra.db.adapters.postgres.types import (
        PostgresPipelineOperation,
        PostgresPipelineOutcome,
        PostgresValue,
    )
    from vertebra.db.cdc import ChangeFeed

__all__ = ("SyncPostgreSQLDriver",)

_DEFAULT_TENANT_SCOPE = TenantScopeConfig()
_DEFAULT_ARROW_MAX_RESULT_BYTES = 268_435_456


def _reject_memory_dsn(dsn: str) -> None:
    """Reject unsupported ``memory://`` DSNs cleanly with ``ImproperConfigurationError``."""
    if dsn.strip().lower().startswith("memory://"):
        msg = "PostgreSQL drivers do not support memory:// DSNs; pass a postgresql:// DSN or inject pool="
        raise ImproperConfigurationError(msg)


def _resolve_native_postgres_module() -> Any:
    """Resolve the compiled ``vertebra._postgres`` extension or raise ``MissingDependencyError``."""
    return ensure_module(
        "vertebra._postgres",
        package="vertebra-core[postgres]",
        extra="postgres",
    )


def _prepare_postgres_query(
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None = None,
) -> tuple[str, tuple[Any, ...]]:
    """Compile SQL placeholders and normalize parameters for native execution and pipelining."""
    compiled = compile_postgres_binds(sql, parameters)
    return compiled.sql, compiled.params


def _project_query_rows(result: PostgresQueryResult) -> tuple[list[tuple[Any, ...]], tuple[str, ...], int]:
    """Convert a native ``PostgresQueryResult`` into ``(rows, column_names, affected_rows)``."""
    cols = tuple(col.name for col in result.columns)
    rows = [_project_public_row(row) for row in result.rows]
    return rows, cols, int(result.affected_rows)


def _arrow_result_to_cursor(result: PostgresArrowResult) -> ArrowCursor:
    """Wrap an inline IPC or native SHM ``PostgresArrowResult`` into ``ArrowCursor`` without row copies."""
    if result.used_shm:
        return ArrowCursor(
            result.buffer,
            columns=result.columns,
            used_shm=True,
            shm_cursor=result.buffer,
            row_count=result.row_count,
        )
    return ArrowCursor(
        result.buffer,
        columns=result.columns,
        used_shm=False,
        row_count=result.row_count,
    )


def _validate_arrow_target_identifiers(
    table_name: str,
    *,
    columns: Sequence[str] | None,
    schema_name: str | None,
) -> None:
    """Validate SQL identifiers passed to bulk Arrow COPY ingestion."""
    if schema_name is not None:
        validate_identifier(schema_name, "schema_name", allow_dots=False)
    validate_identifier(table_name, "table_name", allow_dots=schema_name is None)
    if columns is not None:
        for col in columns:
            validate_identifier(str(col), "column", allow_dots=False)


def _coerce_arrow_source(data: Any, columns: Sequence[str] | None) -> Any | None:
    """Normalize plain row sequences into ``ArrowCursor`` payloads while preserving native Arrow inputs."""
    if (
        isinstance(data, Sequence)
        and not isinstance(data, (str, bytes, bytearray, memoryview, ArrowCursor))
        and not hasattr(data, "__arrow_c_stream__")
        and not hasattr(data, "to_batches")
    ):
        if not data and columns is None:
            return None
        return ArrowCursor.from_rows(data, columns)
    return data


def _validate_direct_session_config(config: PostgreSQLConfig, *, feature: str) -> PostgreSQLConfig:
    """Ensure direct non-pooled configuration is active before opening LISTEN or CDC streams."""
    direct_cfg = config.derive_direct_config()
    _reject_memory_dsn(direct_cfg.dsn)
    if direct_cfg.pooler_mode not in ("direct", "session") or direct_cfg.pgbouncer:
        msg = (
            f"PostgreSQL {feature} requires a direct or session-mode connection "
            f"(got pooler_mode={direct_cfg.pooler_mode!r}); configure direct_dsn for pooled deployments"
        )
        raise ImproperConfigurationError(msg)
    return direct_cfg


def _build_direct_client_kwargs(config: PostgreSQLConfig, *, feature: str) -> dict[str, Any]:
    """Resolve dedicated non-pooled ``AsyncPostgresClient`` keyword arguments from ``config``."""
    direct_cfg = _validate_direct_session_config(config, feature=feature)
    options = build_native_client_options(direct_cfg)
    return options.to_client_kwargs()


def _validate_tenant_setting_name(setting_name: str) -> str:
    """Validate a dotted PostgreSQL GUC setting name for tenant scoping."""
    if not setting_name:
        msg = "Tenant scope setting_name cannot be empty"
        raise ProgrammingError(msg)
    for segment in setting_name.split("."):
        validate_identifier(segment, "setting_name", allow_dots=False)
    return setting_name


def _validate_savepoint_token(token: int | None, allocator: Callable[[], int]) -> int:
    """Validate or allocate a numeric savepoint token."""
    if token is None:
        return allocator()
    if isinstance(token, bool) or not isinstance(token, int) or token < 0:
        msg = "Savepoint token must be a non-negative integer"
        raise ProgrammingError(msg)
    return token


def _chunk_iterable[ItemT](items: Iterable[ItemT], size: int) -> Iterator[list[ItemT]]:
    """Yield fixed-size slices from an arbitrary iterable without full buffering."""
    chunk_limit = max(1, int(size))
    buf: list[ItemT] = []
    for item in items:
        buf.append(item)
        if len(buf) >= chunk_limit:
            yield buf
            buf = []
    if buf:
        yield buf


class _PooledSyncPipelineLease:
    """Lazy pooled client adapter binding ``SyncPostgresPipeline`` to one released lease."""

    __slots__ = ("_acquire", "_client", "_release")

    def __init__(
        self,
        acquire: Callable[[], Any],
        release: Callable[[Any, bool], None],
    ) -> None:
        self._acquire = acquire
        self._release = release
        self._client: Any = None

    def pipeline_begin(self) -> int:
        """Acquire a pooled client when entering the pipeline and open its retained scope."""
        client = self._acquire()
        self._client = client
        try:
            return int(client.pipeline_begin())
        except BaseException:
            self.release(evict=True)
            raise

    def pipeline_results(
        self,
        scope_id: int,
        operations: Sequence[PostgresPipelineOperation],
    ) -> tuple[PostgresPipelineOutcome, ...]:
        """Submit batched pipeline operations on the leased client."""
        outcomes: tuple[PostgresPipelineOutcome, ...] = tuple(self._client.pipeline_results(scope_id, operations))
        return outcomes

    def pipeline_finish(self, scope_id: int, success: bool) -> None:
        """Finish the pipeline scope and evict the client if completion fails."""
        try:
            self._client.pipeline_finish(scope_id, success)
        except BaseException:
            self.release(evict=True)
            raise

    def release(self, *, evict: bool = False) -> None:
        """Return or evict the leased pooled client at most once."""
        client = self._client
        self._client = None
        if client is not None:
            self._release(client, evict)


class SyncPostgreSQLDriver(SyncDatabaseDriver[PostgreSQLConfig]):
    """Synchronous PostgreSQL driver directly wrapping ``SyncPostgresPool`` and ``SyncPostgresClient``."""

    dialect: ClassVar[str] = "postgres"
    dialect_tag: ClassVar[int] = POSTGRES_DIALECT_TAG

    __slots__ = (
        "_active_client",
        "_client_factory",
        "_dialer",
        "_events",
        "_in_transaction",
        "_next_savepoint_token",
        "_owns_pool",
        "_pool",
        "_savepoint_tokens",
        "_session_context",
        "_tenant_conn",
        "_tx_conn",
        "_tx_owns_conn",
    )

    def __init__(
        self,
        config: PostgreSQLConfig | None = None,
        *,
        pool: Any = None,
        dialer: Any = None,
        broker: Any = None,
        client_factory: Callable[..., Any] | None = None,
    ) -> None:
        """Initialize the synchronous PostgreSQL driver without performing network I/O."""
        resolved = config or PostgreSQLConfig()
        _reject_memory_dsn(resolved.dsn)
        if resolved.direct_dsn is not None:
            _reject_memory_dsn(resolved.direct_dsn)
        super().__init__(resolved, broker=broker)
        self._pool = pool
        self._owns_pool = pool is None
        self._dialer = dialer
        self._client_factory = client_factory
        self._events = EventSessionRegistry()
        self._in_transaction = False
        self._tx_conn: Any = None
        self._tx_owns_conn = False
        self._tenant_conn: Any = None
        self._active_client: Any = None
        self._savepoint_tokens: list[int] = []
        self._next_savepoint_token = 0
        self._session_context: dict[str, str] = {}

    @property
    def _config(self) -> PostgreSQLConfig:
        """Return the active ``PostgreSQLConfig``."""
        return self.config

    @property
    def pool(self) -> Any:
        """Return the underlying synchronous connection pool if initialized."""
        return self._pool

    @property
    def dialer(self) -> Any:
        """Return the attached Cloud SQL or AlloyDB dialer if present."""
        return self._dialer

    @property
    def in_transaction(self) -> bool:
        """Return whether an explicit transaction is currently active."""
        return self._in_transaction

    @staticmethod
    def strip_jsonb_header(payload: Buffer) -> memoryview:
        """Strip the 1-byte PostgreSQL binary JSONB version prefix (``\\x01``) with zero copy."""
        return PostgreSQLCore.strip_jsonb_header(payload)

    @staticmethod
    def encode_jsonb(value: Any) -> bytes:
        """Encode a Python value with the 1-byte PostgreSQL ``\\x01`` JSONB version header."""
        return PostgreSQLCore.encode_jsonb(value)

    @staticmethod
    def encode_pgcopy_binary_rows(
        rows: Sequence[Sequence[bytes | str | None]],
        flags: int = 0,
        ext_bytes: bytes = b"",
    ) -> bytes:
        """Encode rows into PostgreSQL binary COPY format."""
        return PostgreSQLCore.encode_pgcopy_rows(rows, flags=flags, ext_bytes=ext_bytes)

    @staticmethod
    def parse_pgcopy_binary_rows(payload: Buffer) -> list[tuple[bytes | None, ...]]:
        """Parse PostgreSQL binary COPY format stream into row tuples of byte values."""
        return PostgreSQLCore.decode_pgcopy_rows(payload)

    def _ensure_pool(self) -> Any:
        """Lazily construct and cache the native ``SyncPostgresPool`` when not injected."""
        if self._pool is not None:
            return self._pool
        pg_mod = _resolve_native_postgres_module()
        pool_kwargs = build_native_client_options(self.config).to_pool_kwargs()
        self._pool = pg_mod.SyncPostgresPool(**pool_kwargs)
        self._owns_pool = True
        return self._pool

    def _ensure_open(self) -> None:
        """Raise ``InterfaceError`` when an operation targets a closed driver."""
        if self._closed:
            msg = "PostgreSQL driver is closed"
            raise InterfaceError(msg)

    def _acquire_session_client(self) -> tuple[Any, bool]:
        """Return ``(client, owns_lease)`` pinning active transaction or tenant sessions."""
        self._ensure_open()
        if self._tx_conn is not None:
            return self._tx_conn, False
        if self._tenant_conn is not None:
            return self._tenant_conn, False
        pool = self._ensure_pool()
        return pool.acquire(), True

    def _release_pooled_client(self, client: Any, *, evict: bool = False) -> None:
        """Return ``client`` to the pool or evict it when state cannot be trusted."""
        if evict and self._pool is not None:
            evict_fn = getattr(self._pool, "evict", None)
            if callable(evict_fn):
                evict_fn(client)
                return
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            close_fn()

    def _next_token(self) -> int:
        """Return the next monotonic numeric savepoint identifier."""
        self._next_savepoint_token += 1
        return self._next_savepoint_token

    def connect(self) -> SyncPostgreSQLDriver:
        """Initialize or reopen the synchronous connection pool."""
        self._closed = False
        self._ensure_pool()
        return self

    def begin(self) -> None:
        """Begin an explicit transaction on a pinned pooled connection."""
        self._ensure_open()
        if self._tx_conn is not None:
            return
        if self._tenant_conn is not None:
            client = self._tenant_conn
            owns_conn = False
        else:
            client = self._ensure_pool().acquire()
            owns_conn = True
        try:
            client.begin()
        except BaseException:
            if owns_conn:
                with contextlib.suppress(Exception):
                    self._release_pooled_client(client, evict=True)
            raise
        self._tx_conn = client
        self._tx_owns_conn = owns_conn
        self._in_transaction = True

    def commit(self) -> None:
        """Commit the active transaction and release its pinned pooled lease."""
        self._savepoint_tokens.clear()
        client = self._tx_conn
        owns_conn = self._tx_owns_conn
        self._tx_conn = None
        self._tx_owns_conn = False
        self._in_transaction = False
        if client is None:
            return
        try:
            client.commit()
        except BaseException:
            if owns_conn:
                with contextlib.suppress(Exception):
                    self._release_pooled_client(client, evict=True)
            raise
        if owns_conn:
            self._release_pooled_client(client, evict=False)

    def rollback(self) -> None:
        """Roll back the active transaction, evicting tainted sessions if rollback fails."""
        self._savepoint_tokens.clear()
        client = self._tx_conn
        owns_conn = self._tx_owns_conn
        self._tx_conn = None
        self._tx_owns_conn = False
        self._in_transaction = False
        if client is None:
            return
        try:
            client.rollback()
        except BaseException:
            if owns_conn:
                with contextlib.suppress(Exception):
                    self._release_pooled_client(client, evict=True)
            else:
                self._evict_pinned_connections()
            raise
        if owns_conn:
            self._release_pooled_client(client, evict=False)

    def savepoint(self, token: int | None = None) -> int:
        """Create a numeric savepoint inside the active transaction."""
        self._ensure_open()
        if not self._in_transaction or self._tx_conn is None:
            msg = "Savepoints require an active PostgreSQL transaction"
            raise InterfaceError(msg)
        resolved = _validate_savepoint_token(token, self._next_token)
        self._tx_conn.savepoint(resolved)
        self._savepoint_tokens.append(resolved)
        return resolved

    def release_savepoint(self, token: int) -> None:
        """Release an active numeric savepoint inside the active transaction."""
        self._ensure_open()
        if not self._in_transaction or self._tx_conn is None:
            msg = "Savepoints require an active PostgreSQL transaction"
            raise InterfaceError(msg)
        resolved = _validate_savepoint_token(token, self._next_token)
        self._tx_conn.release_savepoint(resolved)
        with contextlib.suppress(ValueError):
            self._savepoint_tokens.remove(resolved)

    def rollback_to_savepoint(self, token: int) -> None:
        """Roll back the active transaction state to numeric savepoint ``token``."""
        self._ensure_open()
        if not self._in_transaction or self._tx_conn is None:
            msg = "Savepoints require an active PostgreSQL transaction"
            raise InterfaceError(msg)
        resolved = _validate_savepoint_token(token, self._next_token)
        self._tx_conn.rollback_to_savepoint(resolved)

    def enter_tenant_scope(
        self,
        tenant_value: str,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
    ) -> str:
        """Pin a session and bind ``config.setting_name`` via parameterized ``set_config``."""
        self._ensure_open()
        setting_name = _validate_tenant_setting_name(config.setting_name)
        tenant_str = str(tenant_value)
        if self._tx_conn is not None:
            self.execute("SELECT set_config($1, $2, $3)", (setting_name, tenant_str, True))
            self._session_context[setting_name] = tenant_str
            return tenant_str
        if self._tenant_conn is None:
            self._tenant_conn = self._ensure_pool().acquire()
        try:
            self.execute("SELECT set_config($1, $2, $3)", (setting_name, tenant_str, False))
        except BaseException:
            self.evict_connection()
            raise
        self._session_context[setting_name] = tenant_str
        return tenant_str

    def exit_tenant_scope(self, config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE) -> None:
        """Reset the tenant session setting, evicting the physical session on reset failure."""
        setting_name = _validate_tenant_setting_name(config.setting_name)
        self._session_context.pop(setting_name, None)
        if self._tx_conn is not None:
            try:
                self.execute("SELECT set_config($1, $2, $3)", (setting_name, "", True))
            except BaseException:
                self.evict_connection()
                raise
            return
        tenant_conn = self._tenant_conn
        if tenant_conn is None:
            return
        try:
            self.execute("SELECT set_config($1, $2, $3)", (setting_name, "", False))
        except BaseException:
            self.evict_connection()
            raise
        if not self._session_context:
            self._tenant_conn = None
            self._release_pooled_client(tenant_conn, evict=False)

    def _evict_pinned_connections(self) -> None:
        """Evict any pinned transaction or tenant-scoped session from the pool."""
        tx_conn = self._tx_conn
        tenant_conn = self._tenant_conn
        self._tx_conn = None
        self._tx_owns_conn = False
        self._tenant_conn = None
        self._in_transaction = False
        self._savepoint_tokens.clear()
        if tx_conn is not None:
            with contextlib.suppress(Exception):
                self._release_pooled_client(tx_conn, evict=True)
        if tenant_conn is not None and tenant_conn is not tx_conn:
            with contextlib.suppress(Exception):
                self._release_pooled_client(tenant_conn, evict=True)

    def evict_connection(self) -> None:
        """Clear session tenant context and discard any pinned physical connection."""
        self._session_context.clear()
        self._evict_pinned_connections()

    def cancel(self) -> None:
        """Interrupt an active query or pinned transaction client idempotently."""
        target = self._active_client or self._tx_conn or self._tenant_conn
        if target is None:
            return
        cancel_fn = getattr(target, "cancel", None)
        if callable(cancel_fn):
            cancel_fn()

    def _close_events_sync(self) -> None:
        """Close all tracked event sessions synchronously without masking driver teardown."""
        if not len(self._events):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self._events.close_all())
            return
        loop.create_task(self._events.close_all())

    def close(self) -> None:
        """Roll back open transactions, close tracked event sessions, and drain the pool."""
        was_in_tx = self._in_transaction
        tx_error: BaseException | None = None
        events_error: BaseException | None = None
        pool_error: BaseException | None = None
        self._closed = True
        if was_in_tx:
            try:
                self.rollback()
            except BaseException as exc:
                tx_error = exc
        self._evict_pinned_connections()
        try:
            self._close_events_sync()
        except BaseException as exc:
            events_error = exc
        pool = self._pool
        if pool is not None:
            try:
                if not getattr(pool, "closed", False):
                    pool.close()
            except BaseException as exc:
                pool_error = exc
            finally:
                if self._owns_pool:
                    self._pool = None
        first_error = tx_error or events_error or pool_error
        if first_error is not None:
            raise first_error

    def _run_query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> PostgresQueryResult:
        """Compile parameters, execute on the native client, and honor broker/cancellation rules."""
        timeout = kwargs.pop("timeout", None)
        parameter_oids = kwargs.pop("parameter_oids", None)
        compiled_sql, compiled_params = _prepare_postgres_query(sql, parameters)
        annotated_sql = self._annotate_sql(compiled_sql, **kwargs)
        if self.config.effective_statement_cache_size > 0:
            self._lookup_or_cache_statement(annotated_sql)
        lease = self._acquire_broker_lease()
        client: Any = None
        owns_client = False
        try:
            client, owns_client = self._acquire_session_client()
            prev_active = self._active_client
            self._active_client = client
            try:
                result: PostgresQueryResult = client.query(
                    annotated_sql,
                    compiled_params if compiled_params else None,
                    parameter_oids=parameter_oids,
                    timeout=timeout,
                )
            finally:
                self._active_client = prev_active
        except BaseException:
            if owns_client and client is not None:
                with contextlib.suppress(Exception):
                    self._release_pooled_client(client, evict=True)
            raise
        else:
            if owns_client and client is not None:
                self._release_pooled_client(client, evict=False)
            return result
        finally:
            self._release_broker_lease(lease)

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return affected row count."""
        result = self._run_query(sql, parameters, **kwargs)
        return int(result.affected_rows)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query returning projected tuple rows and ordered column names."""
        result = self._run_query(sql, parameters, **kwargs)
        rows, cols, _ = _project_query_rows(result)
        return rows, cols

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute ``sql`` across all parameter sets using one leased session."""
        self._ensure_open()
        if not parameter_sets:
            return 0
        timeout = kwargs.pop("timeout", None)
        parameter_oids = kwargs.pop("parameter_oids", None)
        compiled_batch = [
            (self._annotate_sql(c_sql, **kwargs), c_params)
            for c_sql, c_params in (_prepare_postgres_query(sql, item) for item in parameter_sets)
        ]
        lease = self._acquire_broker_lease()
        client: Any = None
        owns_client = False
        total = 0
        try:
            client, owns_client = self._acquire_session_client()
            prev_active = self._active_client
            self._active_client = client
            try:
                for batch_sql, batch_params in compiled_batch:
                    result = client.query(
                        batch_sql,
                        batch_params if batch_params else None,
                        parameter_oids=parameter_oids,
                        timeout=timeout,
                    )
                    total += int(result.affected_rows)
            finally:
                self._active_client = prev_active
        except BaseException:
            if owns_client and client is not None:
                with contextlib.suppress(Exception):
                    self._release_pooled_client(client, evict=True)
            raise
        else:
            if owns_client and client is not None:
                self._release_pooled_client(client, evict=False)
            return total
        finally:
            self._release_broker_lease(lease)

    def pipeline(self) -> SyncPostgresPipeline:
        """Open a synchronous pipeline bound to the active transaction or a single pooled lease."""
        self._ensure_open()
        pinned = self._tx_conn or self._tenant_conn
        if pinned is not None:
            return SyncPostgresPipeline(pinned, prepare=_prepare_postgres_query, release=None)
        lease = _PooledSyncPipelineLease(
            lambda: self._ensure_pool().acquire(),
            lambda client, evict: self._release_pooled_client(client, evict=evict),
        )
        return SyncPostgresPipeline(
            lease,
            prepare=_prepare_postgres_query,
            release=lease.release,
        )

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        max_result_bytes: int = _DEFAULT_ARROW_MAX_RESULT_BYTES,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query directly into inline Arrow IPC bytes or a ``VTBARROW`` SHM cursor."""
        timeout = kwargs.pop("timeout", None)
        parameter_oids = kwargs.pop("parameter_oids", None)
        compiled_sql, compiled_params = _prepare_postgres_query(sql, parameters)
        annotated_sql = self._annotate_sql(compiled_sql, **kwargs)
        lease = self._acquire_broker_lease()
        client: Any = None
        owns_client = False
        try:
            client, owns_client = self._acquire_session_client()
            prev_active = self._active_client
            self._active_client = client
            try:
                result: PostgresArrowResult = client.select_to_arrow(
                    annotated_sql,
                    compiled_params if compiled_params else None,
                    parameter_oids=parameter_oids,
                    timeout=timeout,
                    threshold_rows=threshold_rows,
                    max_result_bytes=max_result_bytes,
                )
            finally:
                self._active_client = prev_active
        except BaseException:
            if owns_client and client is not None:
                with contextlib.suppress(Exception):
                    self._release_pooled_client(client, evict=True)
            raise
        else:
            if owns_client and client is not None:
                self._release_pooled_client(client, evict=False)
            return _arrow_result_to_cursor(result)
        finally:
            self._release_broker_lease(lease)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> Generator[ArrowCursor, None, None]:
        """Stream server-portal Arrow SHM batches cleanly while pinning any leased client."""
        parameter_oids = kwargs.pop("parameter_oids", None)
        compiled_sql, compiled_params = _prepare_postgres_query(sql, parameters)
        annotated_sql = self._annotate_sql(compiled_sql, **kwargs)
        self._ensure_open()

        def _iterate() -> Generator[ArrowCursor, None, None]:
            lease = self._acquire_broker_lease()
            client: Any = None
            owns_client = False
            stream: Any = None
            completed = False
            try:
                client, owns_client = self._acquire_session_client()
                stream = client.stream_arrow_batches(
                    annotated_sql,
                    compiled_params if compiled_params else None,
                    parameter_oids=parameter_oids,
                    batch_size=batch_size,
                    columns=columns,
                )
                for native_cursor in stream:
                    cursor_cols: Sequence[str] = (
                        columns if columns is not None else tuple(getattr(native_cursor, "columns", ()))
                    )
                    yield ArrowCursor(
                        native_cursor,
                        columns=cursor_cols,
                        used_shm=True,
                        shm_cursor=native_cursor,
                        row_count=getattr(native_cursor, "row_count", None),
                    )
                completed = True
            finally:
                if stream is not None:
                    with contextlib.suppress(Exception):
                        stream.close()
                if owns_client and client is not None:
                    with contextlib.suppress(Exception):
                        self._release_pooled_client(client, evict=not completed)
                self._release_broker_lease(lease)

        return _iterate()

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        schema_name: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-load an Arrow source or sequence into ``table_name`` over native binary COPY."""
        _ = kwargs
        self._ensure_open()
        _validate_arrow_target_identifiers(table_name, columns=columns, schema_name=schema_name)
        source = _coerce_arrow_source(data, columns)
        if source is None:
            return 0
        lease = self._acquire_broker_lease()
        client: Any = None
        owns_client = False
        try:
            client, owns_client = self._acquire_session_client()
            prev_active = self._active_client
            self._active_client = client
            try:
                inserted = load_arrow_sync(
                    client,
                    table_name,
                    source,
                    columns=columns,
                    schema_name=schema_name,
                )
            finally:
                self._active_client = prev_active
        except BaseException:
            if owns_client and client is not None:
                with contextlib.suppress(Exception):
                    self._release_pooled_client(client, evict=True)
            raise
        else:
            if owns_client and client is not None:
                self._release_pooled_client(client, evict=False)
            return inserted
        finally:
            self._release_broker_lease(lease)

    @contextmanager
    def copy_to(self, sql: str) -> Iterator[Any]:
        """Open a synchronous binary ``COPY ... TO STDOUT`` row stream over a pinned lease."""
        self._ensure_open()
        client, owns_client = self._acquire_session_client()
        stream: Any = None
        failed = False
        try:
            stream = client.copy_to(sql)
            with stream as entered_stream:
                yield entered_stream
        except BaseException:
            failed = True
            raise
        finally:
            if stream is not None:
                with contextlib.suppress(Exception):
                    stream.close()
            if owns_client:
                self._release_pooled_client(client, evict=failed)

    def copy_from(
        self,
        sql: str,
        rows: Iterable[Sequence[PostgresValue]],
        column_oids: Sequence[int] = (),
        *,
        batch_size: int = 1024,
    ) -> int:
        """Stream normalized binary rows into PostgreSQL ``COPY ... FROM STDIN`` cleanly."""
        self._ensure_open()
        client, owns_client = self._acquire_session_client()
        copy_in: Any = None
        failed = False
        try:
            copy_in = client.copy_from(sql)
            for chunk in _chunk_iterable(rows, batch_size):
                copy_in.write(chunk, column_oids)
            return int(copy_in.finish())
        except BaseException:
            failed = True
            if copy_in is not None:
                with contextlib.suppress(Exception):
                    copy_in.close()
            raise
        finally:
            if owns_client:
                self._release_pooled_client(client, evict=failed)

    async def _open_direct_async_client(self, *, feature: str) -> Any:
        """Dial and track a dedicated non-pooled ``AsyncPostgresClient`` for event streaming."""
        self._ensure_open()
        client_kwargs = _build_direct_client_kwargs(self.config, feature=feature)
        if self._client_factory is not None:
            client = self._client_factory(**client_kwargs)
        else:
            pg_mod = _resolve_native_postgres_module()
            client = pg_mod.AsyncPostgresClient(**client_kwargs)
        await client.connect()
        return client

    def listen_notifications(
        self,
        config: PostgresNotifyConfig | None = None,
        *,
        change_feed: ChangeFeed | None = None,
        queue_capacity: int = 1024,
        reconnect: bool = False,
    ) -> PostgresNotificationSource:
        """Create a ``PostgresNotificationSource`` backed by dedicated non-pooled listener connections."""
        self._ensure_open()
        _validate_direct_session_config(self.config, feature="LISTEN/NOTIFY")
        return PostgresNotificationSource(
            self,
            config,
            change_feed=change_feed,
            queue_capacity=queue_capacity,
            reconnect=reconnect,
        )

    async def _open_notification_listener(
        self,
        channels: Sequence[str],
        *,
        queue_capacity: int = 1024,
        reconnect: bool = False,
    ) -> AsyncPostgresListener:
        """Open a dedicated non-pooled listener and register it for deterministic driver cleanup."""
        client = await self._open_direct_async_client(feature="LISTEN/NOTIFY")
        try:
            listener: AsyncPostgresListener = await client.open_listener(
                channels,
                queue_capacity=queue_capacity,
                reconnect=reconnect,
            )
        except BaseException:
            with contextlib.suppress(Exception):
                await client.close()
            raise
        self._events.register(listener, client)
        return listener

    async def _publish_notification(self, channel: str, payload: str) -> None:
        """Publish a parameterized PostgreSQL ``pg_notify`` message off the event loop."""
        await asyncio.to_thread(self.execute, "SELECT pg_notify($1, $2)", (channel, payload))

    def open_replication(
        self,
        config: PostgresChangeConfig,
        checkpoint_store: PostgresCheckpointStore | None = None,
        *,
        feed: ChangeFeed | None = None,
    ) -> PostgresChangeSource:
        """Create a ``PostgresChangeSource`` backed by dedicated non-pooled WAL replication streams."""
        self._ensure_open()
        _validate_direct_session_config(self.config, feature="logical replication")
        return PostgresChangeSource(self, config, checkpoint_store, feed=feed)

    async def _replication_identity(
        self,
        slot: str,
        publication: str,
    ) -> PostgresReplicationIdentity:
        """Query server/slot/publication identity over a dedicated non-pooled direct client."""
        client = await self._open_direct_async_client(feature="logical replication")
        try:
            identity: PostgresReplicationIdentity = await client.replication_identity(slot, publication)
            return identity
        finally:
            with contextlib.suppress(Exception):
                await client.close()

    async def _open_replication_stream(
        self,
        config: PostgresChangeConfig,
        *,
        start_lsn: int | None = None,
        reconnect: bool = False,
    ) -> AsyncPostgresReplication:
        """Open a dedicated non-pooled logical replication stream and track its cleanup."""
        client = await self._open_direct_async_client(feature="logical replication")
        try:
            stream: AsyncPostgresReplication = await client.open_replication(
                config.slot,
                config.publication,
                start_lsn=start_lsn,
                queue_capacity=config.queue_capacity,
                reconnect=reconnect,
            )
        except BaseException:
            with contextlib.suppress(Exception):
                await client.close()
            raise
        self._events.register(stream, client)
        return stream
