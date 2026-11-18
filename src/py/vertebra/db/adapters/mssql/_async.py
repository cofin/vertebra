"""Asynchronous pure-Rust SQL Server (TDS 7.4) driver."""

from __future__ import annotations

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
    _hydrate_rows_with_schema,
)
from vertebra.db.driver._async import AsyncDatabaseDriver
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._mssql import AsyncMSSQLClient
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
        "_last_rewritten_sql",
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
        self._last_rewritten_sql: str = ""

    @property
    def dialer(self) -> AsyncCloudSQLDialer | None:
        """Return the attached asynchronous Cloud SQL dialer if present."""
        return self._dialer

    @property
    def last_rewritten_sql(self) -> str:
        """Return the most recently rewritten `@P1` T-SQL statement."""
        return self._last_rewritten_sql

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
        self._closed = False
        return self

    async def ping(self) -> bool:
        """Execute an asynchronous health check against the SQL Server session."""
        return bool(await self._run_with_client(lambda client: client.ping(), use_broker_lease=False))

    async def begin(self, isolation_level: str | None = None) -> None:
        """Begin an explicit SQL Server transaction."""

        async def _op(client: Any) -> None:
            if isolation_level is not None:
                await client.begin(isolation_level)
            else:
                await client.begin()

        await self._run_with_client(_op, use_broker_lease=False)
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
        await self._client.close()

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement asynchronously and return affected row count."""
        _ = kwargs
        rewritten = self._prepare_sql(sql)
        return int(
            await self._run_with_client(
                lambda client: client.execute(rewritten, parameters),
                use_broker_lease=True,
            )
        )

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across multiple parameter sets asynchronously."""
        _ = kwargs
        if not parameter_sets:
            return 0
        rewritten = self._prepare_sql(sql)
        param_list = list(parameter_sets)
        return int(
            await self._run_with_client(
                lambda client: client.execute_many(rewritten, param_list),
                use_broker_lease=True,
            )
        )

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query asynchronously and return positional tuples."""
        rows, _ = await self.execute_with_description(sql, parameters, **kwargs)
        return rows

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL asynchronously and return `(rows, column_names)`."""
        _ = kwargs
        rewritten = self._prepare_sql(sql)

        async def _op(client: Any) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
            if hasattr(client, "execute_with_description"):
                rows, cols = await client.execute_with_description(rewritten, parameters)
                return list(rows), tuple(cols)
            raw = await client.query(rewritten, parameters)
            cols = tuple(str(c) for c in raw.get("columns", ()))
            rows = [tuple(r) for r in raw.get("rows", ())]
            return rows, cols

        return await self._run_with_client(_op, use_broker_lease=True)

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
        if schema_type is None or schema_type is dict:
            rewritten = self._prepare_sql(sql)
            return list(
                await self._run_with_client(
                    lambda client: client.fetch_all(rewritten, parameters),
                    use_broker_lease=True,
                )
            )
        rows, cols = await self.execute_with_description(sql, parameters)
        return _hydrate_rows_with_schema(rows, cols, schema_type)

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
        if schema_type is None or schema_type is dict:
            rewritten = self._prepare_sql(sql)
            return await self._run_with_client(
                lambda client: client.fetch_one(rewritten, parameters),
                use_broker_lease=True,
            )
        rows = await self.select(sql, parameters, schema_type=schema_type)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

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
        if schema_type is None or schema_type is dict:
            rewritten = self._prepare_sql(sql)
            return await self._run_with_client(
                lambda client: client.fetch_optional(rewritten, parameters),
                use_broker_lease=True,
            )
        rows = await self.select(sql, parameters, schema_type=schema_type)
        return rows[0] if rows else None

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
        rewritten = self._prepare_sql(sql)
        val = await self._run_with_client(
            lambda client: client.select_value(rewritten, parameters),
            use_broker_lease=True,
        )
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
        rewritten = self._prepare_sql(sql)
        raw_cursor = await self._run_with_client(
            lambda client: client.select_to_arrow(rewritten, parameters),
            use_broker_lease=True,
        )
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
        rewritten = self._prepare_sql(sql)
        raw_batches = await self._run_with_client(
            lambda client: client.stream_arrow_batches(rewritten, parameters, chunk),
            use_broker_lease=True,
        )
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
        self._prepare_sql(f"INSERT BULK {table_name}")
        return int(
            await self._run_with_client(
                lambda client: client.load_from_arrow(table_name, source, col_list),
                use_broker_lease=True,
            )
        )

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

    async def _run_with_client[T](
        self,
        op: Callable[[Any], Awaitable[T]],
        *,
        use_broker_lease: bool = False,
    ) -> T:
        for attempt in (0, 1):
            if self.config.token_provider is not None and not self.in_transaction:
                await self._refresh_aad_token_async()
            grant = self._acquire_broker_lease() if use_broker_lease else None
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
            finally:
                if use_broker_lease:
                    self._release_broker_lease(grant)
        msg = "MSSQL authentication retry exhausted"
        raise OperationalError(msg)

    def _prepare_sql(self, sql: str) -> str:
        rewritten, param_count = MSSQLCore.rewrite_placeholders(sql)
        plan = self._lookup_or_cache_statement(
            sql,
            param_count=param_count,
            compiled_sql=rewritten.encode("utf-8"),
        )
        if plan is not None and plan.compiled_sql:
            rewritten = plan.compiled_sql.decode("utf-8")
        self._last_rewritten_sql = rewritten
        return rewritten
