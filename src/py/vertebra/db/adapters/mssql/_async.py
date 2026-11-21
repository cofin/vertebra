"""Asynchronous pure-Rust SQL Server (TDS 7.4) driver."""

from __future__ import annotations

import contextlib
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Self

from vertebra._vertebra import NotFoundError, OperationalError
from vertebra.db.adapters.mssql.config import MSSQLConfig
from vertebra.db.adapters.mssql.core import (
    MSSQL_DIALECT_TAG,
    MSSQLCore,
)
from vertebra.db.adapters.mssql.pipeline import AsyncMSSQLPipeline
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
)
from vertebra.db.driver._async import AsyncDatabaseDriver
from vertebra.db.driver._common import _lease_releaser
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._mssql import AsyncMSSQLClient, AsyncMSSQLCursor
    from vertebra.db.dialer.cloudsql import AsyncCloudSQLDialer

__all__ = ("AsyncMSSQLDriver",)


class AsyncMSSQLDriver(AsyncDatabaseDriver[MSSQLConfig]):
    """Asynchronous SQL Server TDS 7.4 driver backed by `vertebra._mssql.AsyncMSSQLClient`."""

    dialect: ClassVar[str] = "mssql"
    dialect_tag: ClassVar[int] = MSSQL_DIALECT_TAG

    __slots__ = (
        "_client",
        "_client_token",
        "_dialer",
        "_in_transaction",
    )

    def __init__(
        self,
        config: MSSQLConfig | str | None = None,
        *,
        dialer: AsyncCloudSQLDialer | None = None,
        broker: Any = None,
        client: AsyncMSSQLClient | Any | None = None,
        **overrides: Any,
    ) -> None:
        resolved = MSSQLConfig._resolve_driver_config(config, overrides, dialer)
        super().__init__(resolved, broker=broker)
        self._dialer = dialer
        self._client: AsyncMSSQLClient | Any = (
            client
            if client is not None
            else MSSQLCore.create_async_rust_client(
                resolved,
                dialer=dialer,
                resolve_token=(resolved.token_provider is None),
            )
        )
        self._client_token: str | None = None
        self._in_transaction: bool = False

    @property
    def dialer(self) -> AsyncCloudSQLDialer | None:
        """Return the attached asynchronous Cloud SQL dialer if present."""
        return self._dialer

    @property
    def in_transaction(self) -> bool:
        """Return whether an explicit transaction is currently open."""
        if self._in_transaction:
            return True
        if hasattr(self._client, "in_transaction"):
            return bool(self._client.in_transaction)
        return False

    @property
    def routing_destination(self) -> tuple[str, int] | None:
        """Return the latest `(host, port)` received via TDS `ENVCHANGE` routing."""
        dest = getattr(self._client, "routing_destination", None)
        if dest is not None and isinstance(dest, tuple) and len(dest) == 2:
            return str(dest[0]), int(dest[1])
        return None

    def cursor(self, *, lazy_begin: bool = False) -> AsyncMSSQLCursor:
        """Return a native `AsyncMSSQLCursor` bound to this driver's client."""
        self._ensure_open()
        cur: AsyncMSSQLCursor = self._client.cursor(lazy_begin=lazy_begin)
        return cur

    def pipeline(self) -> AsyncMSSQLPipeline:
        """Create an asynchronous transactional query pipeline bound to this driver."""
        return AsyncMSSQLPipeline(self)

    async def connect(self) -> Self:
        """Ensure the underlying native `AsyncMSSQLClient` is ready with a fresh Entra token if configured."""
        if self._client is None or getattr(self._client, "closed", False):
            token = await self.config.resolve_aad_token_async()
            self._client = MSSQLCore.create_async_rust_client(
                self.config,
                dialer=self._dialer,
                aad_token=token,
                resolve_token=False,
            )
            self._client_token = token
        else:
            await self._refresh_aad_token_async()
        if self._broker is not None:
            await self._attach_checkout_lease_async(self._client)
        self._closed = False
        return self

    async def ping(self) -> bool:
        """Execute an asynchronous health check against the SQL Server session."""
        return bool(await self._run_with_client(lambda client: client.ping()))

    async def begin(self, isolation_level: str | None = None) -> None:
        """Begin an explicit SQL Server transaction."""

        async def _op(client: Any) -> None:
            if isolation_level is not None:
                await client.begin(isolation_level)
            else:
                await client.begin()

        await self._run_with_client(_op)
        self._in_transaction = True

    async def commit(self) -> None:
        """Commit the active SQL Server transaction."""
        try:
            await self._client.commit()
        finally:
            self._in_transaction = False

    async def rollback(self) -> None:
        """Roll back the active SQL Server transaction."""
        try:
            await self._client.rollback()
        finally:
            self._in_transaction = False

    async def savepoint(self, name: str) -> None:
        """Create a transaction savepoint (`SAVE TRANSACTION`)."""
        validate_identifier(name, "savepoint")
        await self._client.savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a transaction savepoint (`ROLLBACK TRANSACTION <name>`)."""
        validate_identifier(name, "savepoint")
        await self._client.rollback_to_savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        """Validate a savepoint identifier (no-op on T-SQL)."""
        validate_identifier(name, "savepoint")
        await self._client.release_savepoint(name)

    async def cancel(self) -> None:
        """Send an out-of-band TDS `ATTENTION` (`0x06`) packet to interrupt an in-flight query."""
        if not self._closed:
            await self._client.cancel()

    async def close(self) -> None:
        """Close the pooled TDS sessions and release driver resources."""
        self._closed = True
        self._in_transaction = False
        if self._broker is not None:
            release_fn = getattr(self._client, "release_lease", None)
            if callable(release_fn):
                release_fn()
        await self._client.close()

    def _ensure_open(self) -> None:
        if self._closed or self._client is None or bool(getattr(self._client, "closed", False)):
            msg = "AsyncMSSQLDriver is closed"
            raise OperationalError(msg)

    async def _acquire_broker_lease_async(self, timeout_ms: int = 5000, bind_key_hash: int = 0) -> Any:
        broker = self._broker
        if broker is None:
            return None
        plan = self._lease_plan()
        if plan.has_acquire:
            try:
                if plan.acquire_is_coroutine:
                    return await broker.acquire(os.getpid(), **plan.acquire_kwargs(timeout_ms, bind_key_hash))
                return broker.acquire(os.getpid(), **plan.acquire_kwargs(timeout_ms, bind_key_hash))
            except RuntimeError:
                return None
        lease_table = getattr(broker, "lease_table", None)
        if lease_table is not None:
            with contextlib.suppress(RuntimeError):
                return lease_table.acquire(os.getpid())
        return None

    async def _attach_checkout_lease_async(self, client: Any, timeout_ms: int = 5000) -> bool:
        lease = await self._acquire_broker_lease_async(timeout_ms)
        if lease is None:
            return False
        client.attach_lease(_lease_releaser(self._broker, self._lease_plan(), lease))
        return True

    async def _execute_cursor(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> AsyncMSSQLCursor:
        self._ensure_open()
        bound = parameters or ()
        cur: AsyncMSSQLCursor = await self._run_with_client(lambda client: client.cursor().execute(sql, bound))
        return cur

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement asynchronously and return affected row count."""
        _ = kwargs
        cur = await self._execute_cursor(sql, parameters)
        return max(int(cur.rowcount), 0)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across multiple parameter sets asynchronously."""
        _ = kwargs
        self._ensure_open()
        if not parameter_sets:
            return 0
        param_list = list(parameter_sets)
        cur = await self._run_with_client(lambda client: client.cursor().executemany(sql, param_list))
        return max(int(cur.rowcount), 0)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query asynchronously and return positional tuples."""
        _ = kwargs
        cur = await self._execute_cursor(sql, parameters)
        if cur.description is None:
            return []
        rows: list[tuple[Any, ...]] = await cur.fetchall()
        return rows

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL asynchronously and return `(rows, column_names)`."""
        _ = kwargs
        cur = await self._execute_cursor(sql, parameters)
        description = cur.description
        if description is None:
            return [], ()
        names = tuple(col[0] for col in description)
        rows: list[tuple[Any, ...]] = await cur.fetchall()
        return rows, names

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query asynchronously and return rows as dictionaries or `schema_type` instances."""
        _ = kwargs
        cur = await self._execute_cursor(sql, parameters)
        description = cur.description
        if description is None:
            return []
        names = tuple(col[0] for col in description)
        rows = await cur.fetchall()
        return self.hydrate_rows(rows, names, schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query asynchronously and return the first row, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        cur = await self._execute_cursor(sql, parameters)
        description = cur.description
        row = await cur.fetchone() if description is not None else None
        if row is None or description is None:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        names = tuple(col[0] for col in description)
        return self.hydrate_rows((row,), names, schema_type)[0]

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query asynchronously and return the first row or `None` when empty."""
        _ = kwargs
        cur = await self._execute_cursor(sql, parameters)
        description = cur.description
        row = await cur.fetchone() if description is not None else None
        if row is None or description is None:
            return None
        names = tuple(col[0] for col in description)
        return self.hydrate_rows((row,), names, schema_type)[0]

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query asynchronously and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        cur = await self._execute_cursor(sql, parameters)
        row = await cur.fetchone() if cur.description is not None else None
        if row is None:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        val = row[0] if row else None
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    async def select_with_total(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[Any], int]:
        """Execute a query asynchronously and return `(rows, total_count)`."""
        rows = await self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows, len(rows)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query asynchronously and materialize the result set as a zero-copy `VTBARROW` `ArrowCursor`."""
        _ = (threshold_rows, kwargs)
        raw_cursor = await self._run_with_client(lambda client: client.select_to_arrow(sql, parameters))
        if isinstance(raw_cursor, ArrowCursor):
            return raw_cursor
        return ArrowCursor(raw_cursor)

    async def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[ArrowCursor]:
        """Execute a query asynchronously and yield `VTBARROW` `ArrowCursor` chunks."""
        _ = kwargs
        chunk = max(1, int(batch_size))
        raw_batches = await self._run_with_client(lambda client: client.stream_arrow_batches(sql, parameters, chunk))
        for item in raw_batches:
            yield item if isinstance(item, ArrowCursor) else ArrowCursor(item)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, IPC stream, or row sequence into `table_name` via TDS `BulkLoad`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        source, col_list = MSSQLCore.coerce_arrow_source(data, columns)
        if source is None:
            return 0
        return int(await self._run_with_client(lambda client: client.load_from_arrow(table_name, source, col_list)))

    def lease_stats(self) -> dict[str, int]:
        """Return broker connection lease telemetry counters."""
        return dict(self._client.lease_stats())

    def statement_cache_stats(self) -> dict[str, int] | None:
        """Return broker statement cache telemetry counters."""
        stats = self._client.statement_cache_stats()
        return dict(stats) if stats is not None else None

    async def _refresh_aad_token_async(self, *, force_refresh: bool = False) -> None:
        if self.config.token_provider is None or self._client is None:
            return
        token = await self.config.resolve_aad_token_async(force_refresh=force_refresh)
        if token != self._client_token:
            set_fn = getattr(self._client, "set_aad_token", None)
            if callable(set_fn):
                set_fn(token)
            self._client_token = token

    async def _run_with_client[T](self, op: Callable[[Any], Awaitable[T]]) -> T:
        for attempt in (0, 1):
            if self.config.token_provider is not None and not self.in_transaction:
                await self._refresh_aad_token_async()
            try:
                return await op(self._client)
            except Exception as exc:
                if (
                    attempt == 0
                    and self.config.token_provider is not None
                    and not self.in_transaction
                    and MSSQLCore.is_auth_rejection(exc)
                ):
                    invalidate_fn = getattr(self.config.token_provider, "invalidate", None)
                    if callable(invalidate_fn):
                        invalidate_fn()
                    await self._refresh_aad_token_async(force_refresh=True)
                    continue
                raise
        msg = "MSSQL authentication retry exhausted"
        raise OperationalError(msg)
