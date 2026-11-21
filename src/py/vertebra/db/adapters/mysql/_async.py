"""Asynchronous native MySQL 8.4/9 and MariaDB driver backed by `vertebra._mysql`."""

from __future__ import annotations

import contextlib
import inspect
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Self

import msgspec
from anyio import CancelScope

from vertebra.db.adapters.mysql.config import (
    MySQLConfig,
    build_mysql_client_kwargs,
    is_mysql_auth_rejection,
)
from vertebra.db.adapters.mysql.core import MySQLCore
from vertebra.db.adapters.mysql.pipeline import AsyncMySQLPipeline
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.db.driver import (
    AZURE_MYSQL_SCOPE,
    AsyncDatabaseDriver,
    AzureEntraTokenProvider,
    DatabaseError,
    NotFoundError,
    OperationalError,
)
from vertebra.db.driver._common import MYSQL_DIALECT_TAG, _lease_releaser

if TYPE_CHECKING:
    from vertebra._mysql import AsyncMySQLCursor

__all__ = (
    "AsyncMySQLDriver",
    "AsyncMySQLPipeline",
    "MySQLConfig",
)


class AsyncMySQLDriver(AsyncDatabaseDriver[MySQLConfig]):
    """Asynchronous MySQL 8.4/9 and MariaDB driver backed by `vertebra._mysql.AsyncMySQLClient`."""

    dialect: ClassVar[str] = "mysql"
    dialect_tag: ClassVar[int] = MYSQL_DIALECT_TAG

    def __init__(
        self,
        config: MySQLConfig | None = None,
        *,
        client: Any = None,
        dialer: Any = None,
        broker: Any = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            config = MySQLConfig(**kwargs) if kwargs else MySQLConfig()
        elif kwargs:
            merged = {f: getattr(config, f) for f in config.__struct_fields__}
            merged.update(kwargs)
            config = MySQLConfig(**merged)

        if isinstance(config.token_provider, AzureEntraTokenProvider):
            config = msgspec.structs.replace(
                config,
                token_provider=config.token_provider.with_default_scope(AZURE_MYSQL_SCOPE),
            )

        super().__init__(config, broker=broker)
        self._client: Any = client
        self._owns_client: bool = client is None
        self._client_token: str | None = None
        self._dialer: Any = dialer
        self._savepoint_seq: int = 0
        self._lastrowid: int | None = None
        self._has_broker: bool = self._broker is not None

    def __repr__(self) -> str:
        """Return a redacted representation of the driver."""
        provider_name = type(self.config.token_provider).__name__ if self.config.token_provider is not None else None
        return (
            f"AsyncMySQLDriver(host={self.config.host!r}, port={self.config.port}, "
            f"user={self.config.user!r}, database={self.config.database!r}, "
            f"ssl_mode={self.config.canonical_ssl_mode!r}, password='***', "
            f"token_provider={provider_name!r})"
        )

    @property
    def dialer(self) -> Any:
        """Return the optional Cloud SQL dialer attached to this driver."""
        return self._dialer

    @property
    def raw_client(self) -> Any:
        """Return the underlying `vertebra._mysql.AsyncMySQLClient` instance."""
        return self._get_client()

    @property
    def in_transaction(self) -> bool:
        """Return whether an explicit transaction is active on this client."""
        if self._client is None:
            return False
        return bool(getattr(self._client, "in_transaction", False))

    @property
    def connection_id(self) -> int | None:
        """Return the active MySQL server `CONNECTION_ID()` when connected."""
        if self._client is None:
            return None
        cid = getattr(self._client, "connection_id", None)
        return int(cid) if cid is not None else None

    @property
    def lastrowid(self) -> int | None:
        """Return the `AUTO_INCREMENT` ID from the most recent `INSERT` statement."""
        return self._lastrowid

    def cursor(self, *, lazy_begin: bool = False) -> AsyncMySQLCursor:
        """Return a native `AsyncMySQLCursor` bound to this driver's client."""
        self._ensure_open()
        cur: AsyncMySQLCursor = self._get_client().cursor(lazy_begin=lazy_begin)
        return cur

    def statement_cache_len(self) -> int:
        """Return the number of binary prepared statements cached in the session LRU."""
        if self._client is None:
            return 0
        fn: Callable[[], Any] | None = getattr(self._client, "statement_cache_len", None)
        return int(fn()) if callable(fn) else 0

    async def connect(self) -> Self:
        """Initialize the underlying native MySQL client and return `self`."""
        self._closed = False
        await self._get_client_async()
        return self

    async def close(self) -> None:
        """Close the underlying native MySQL pool and connections."""
        if self._closed:
            return
        self._closed = True
        client = self._client
        if self._owns_client or (client is not None and bool(getattr(client, "is_closed", False))):
            self._client = None
            self._client_token = None
        if client is not None:
            if self._has_broker:
                release_fn: Callable[[], Any] | None = getattr(client, "release_lease", None)
                if callable(release_fn):
                    release_fn()
            close_fn: Callable[[], Any] | None = getattr(client, "close", None)
            if callable(close_fn):
                res = close_fn()
                if inspect.isawaitable(res):
                    await res

    async def begin(
        self,
        isolation_level: str | None = None,
        *,
        readonly: bool = False,
    ) -> None:
        """Start an explicit transaction on a pinned connection."""

        async def _op(client: Any) -> None:
            try:
                await client.begin(isolation_level, readonly)
            except TypeError:
                await client.begin()

        await self._run_with_client(_op)

    async def commit(self) -> None:
        """Commit the active transaction and return the connection to the pool."""
        await self._run_with_client(lambda client: client.commit())

    async def rollback(self) -> None:
        """Roll back the active transaction and return the connection to the pool."""
        await self._run_with_client(lambda client: client.rollback())

    async def savepoint(self, name: str | None = None) -> str:
        """Create a `SAVEPOINT` inside the current transaction and return its identifier."""
        self._ensure_open()
        if name is None:
            self._savepoint_seq += 1
            name = f"vtb_sp_{self._savepoint_seq}"
        sp = await self._run_with_client(lambda client: client.savepoint(name))
        return str(sp)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back changes to savepoint `name`."""
        await self._run_with_client(lambda client: client.rollback_to_savepoint(name))

    async def release_savepoint(self, name: str) -> None:
        """Release savepoint `name`."""
        await self._run_with_client(lambda client: client.release_savepoint(name))

    @asynccontextmanager
    async def transaction(
        self,
        isolation_level: str | None = None,
        *,
        readonly: bool = False,
    ) -> AsyncIterator[Self]:
        """Async context manager for transactions with shielded rollback and nested `SAVEPOINT` support."""
        await self._get_client_async()
        if self.in_transaction:
            sp_name = await self.savepoint()
            try:
                yield self
            except BaseException:
                with CancelScope(shield=True):
                    await self.rollback_to_savepoint(sp_name)
                    await self.release_savepoint(sp_name)
                raise
            else:
                await self.release_savepoint(sp_name)
        else:
            await self.begin(isolation_level=isolation_level, readonly=readonly)
            try:
                yield self
            except BaseException:
                with CancelScope(shield=True):
                    await self.rollback()
                raise
            else:
                await self.commit()

    def pipeline(self) -> AsyncMySQLPipeline:
        """Return an `AsyncMySQLPipeline` context manager for batched transaction execution."""
        self._ensure_open()
        if self.config.token_provider is None:
            self._get_client()
        return AsyncMySQLPipeline(self)

    async def ping(self) -> bool:
        """Issue a MySQL `COM_PING` health check."""
        if self._closed:
            return False
        return bool(await self._run_with_client(lambda client: client.ping()))

    async def cancel_query(self, connection_id: int | None = None) -> bool:
        """Issue an out-of-band `KILL QUERY <conn_id>` and return whether a target was signalled."""
        if self._closed or self._client is None:
            return False
        try:
            if connection_id is not None and hasattr(self._client, "cancel_query"):
                return bool(await self._client.cancel_query(int(connection_id)))
            return bool(await self._client.cancel())
        except Exception as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise MySQLCore.map_exception(exc) from exc

    async def cancel(self, connection_id: int | None = None) -> None:
        """Issue an out-of-band `KILL QUERY <conn_id>` on a separate control socket."""
        await self.cancel_query(connection_id=connection_id)

    async def _execute_cursor(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Any:
        self._ensure_open()
        bound = parameters or ()
        cur = await self._run_with_client(
            lambda client: client.cursor().execute(sql, bound),
            map_errors=False,
        )
        self._lastrowid = cur.lastrowid
        return cur

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        cur = await self._execute_cursor(sql, parameters)
        return max(int(cur.rowcount), 0)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement repeatedly across `parameter_sets` and return total affected rows."""
        self._ensure_open()
        if not parameter_sets:
            return 0
        cur = await self._run_with_client(
            lambda client: client.cursor().executemany(sql, parameter_sets),
            map_errors=False,
        )
        self._lastrowid = cur.lastrowid
        return max(int(cur.rowcount), 0)

    async def execute_script(self, sql: str) -> int:
        """Execute a semicolon-delimited SQL script via `COM_QUERY` multi-statement mode."""
        client = await self._get_client_async()
        if hasattr(client, "execute_script"):
            return int(await self._run_with_client(lambda c: c.execute_script(sql)))
        try:
            total = 0
            for stmt in (part.strip() for part in sql.split(";") if part.strip()):
                total += await self.execute(stmt)
            return total
        except Exception as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise MySQLCore.map_exception(exc) from exc

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional row tuples."""
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
        """Execute a statement and return `(tuple_rows, column_names)` preserving empty-result columns."""
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
        """Execute a `SELECT` query and return rows as `list[dict[str, Any]]` or hydrated `schema_type`."""
        rows, names = await self.execute_with_description(sql, parameters, **kwargs)
        return self.hydrate_rows(rows, names, schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query and return the first row or raise `NotFoundError`."""
        cur = await self._execute_cursor(sql, parameters)
        description = cur.description
        row = await cur.fetchone() if description is not None else None
        if row is None or description is None:
            err = NotFoundError("Expected at least one row, got 0")
            err.sqlstate = "02000"
            raise err
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
        """Execute a `SELECT` query and return the first row or `None` when empty."""
        cur = await self._execute_cursor(sql, parameters)
        description = cur.description
        row = await cur.fetchone() if description is not None else None
        if row is None or description is None:
            return None
        names = tuple(col[0] for col in description)
        return self.hydrate_rows((row,), names, schema_type)[0]

    async def select_many(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        size: int = 100,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and return at most `size` rows."""
        cur = await self._execute_cursor(sql, parameters)
        description = cur.description
        if description is None:
            return []
        rows = await cur.fetchmany(size)
        names = tuple(col[0] for col in description)
        return self.hydrate_rows(rows, names, schema_type)

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query and return the first scalar value or raise `NotFoundError`."""
        cur = await self._execute_cursor(sql, parameters)
        row = await cur.fetchone() if cur.description is not None else None
        if row is None:
            err = NotFoundError("Expected at least one row, got 0")
            err.sqlstate = "02000"
            raise err
        val = row[0] if row else None
        return MySQLCore.coerce_scalar_value(val, value_type)

    async def select_value_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query and return the first scalar value or `None` when empty."""
        cur = await self._execute_cursor(sql, parameters)
        row = await cur.fetchone() if cur.description is not None else None
        if row is None:
            return None
        val = row[0] if row else None
        return MySQLCore.coerce_scalar_value(val, value_type)

    async def select_with_total(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[Any], int]:
        """Execute a `SELECT` query and return `(rows, total_count)`."""
        rows = await self.select(sql, parameters, schema_type=schema_type, **kwargs)
        cleaned = sql.strip().rstrip(";")
        upper = cleaned.upper()
        if " LIMIT " in upper:
            limit_idx = upper.rfind(" LIMIT ")
            base_sql = cleaned[:limit_idx]
            count_sql = f"SELECT COUNT(*) AS total FROM ({base_sql}) AS _vtb_count_subq"
            total_val = await self.select_value(count_sql, parameters)
            return rows, int(total_val or 0)
        return rows, len(rows)

    async def fetch_all_sets(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[list[dict[str, Any]]]:
        """Execute a multi-result query or `CALL` procedure and return all result sets."""
        self._ensure_open()
        compiled_sql, params = MySQLCore.compile_for_driver(sql, parameters)
        bound = params if params else None
        sets = await self._run_with_client(lambda client: client.fetch_all_sets(compiled_sql, bound))
        return [[dict(r) for r in rs] for rs in sets]

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a `SELECT` query and materialize results into a `VTBARROW` `ArrowCursor`."""
        self._ensure_open()
        compiled_sql, params = MySQLCore.compile_for_driver(sql, parameters)
        bound = params if params else None
        native_cursor = await self._run_with_client(
            lambda client: client.select_to_arrow(compiled_sql, bound, batch_size=batch_size)
        )
        if isinstance(native_cursor, ArrowCursor):
            return native_cursor
        return ArrowCursor(native_cursor, used_shm=True, shm_cursor=native_cursor)

    async def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[ArrowCursor]:
        """Stream a `SELECT` query as bounded `VTBARROW` `ArrowCursor` chunks."""
        self._ensure_open()
        compiled_sql, params = MySQLCore.compile_for_driver(sql, parameters)
        bound = params if params else None
        batches = await self._run_with_client(
            lambda client: client.stream_arrow_batches(compiled_sql, bound, batch_size=batch_size)
        )
        for batch in batches:
            if isinstance(batch, ArrowCursor):
                yield batch
            else:
                yield ArrowCursor(batch, used_shm=True, shm_cursor=batch)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        batch_size: int = 1000,
        **kwargs: Any,
    ) -> int:
        """Bulk-load an `ArrowCursor`, PyArrow Table/RecordBatch, or `__arrow_c_stream__` into `table_name`."""
        self._ensure_open()
        source = data._shm_cursor if isinstance(data, ArrowCursor) and data._shm_cursor is not None else data
        col_list = list(columns) if columns is not None else None
        return int(
            await self._run_with_client(
                lambda client: client.load_from_arrow(
                    table_name,
                    source,
                    columns=col_list,
                    batch_size=batch_size,
                )
            )
        )

    def _ensure_open(self) -> None:
        if self._closed or (self._client is not None and bool(getattr(self._client, "is_closed", False))):
            msg = "AsyncMySQLDriver is closed"
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

    def _create_native_client(self, token: str | None, *, skip_iam_sync: bool = False) -> Any:
        mod = MySQLCore.require_native_module()
        connect_kwargs = build_mysql_client_kwargs(self.config, token, skip_iam_sync=skip_iam_sync)
        self._client = mod.AsyncMySQLClient(**connect_kwargs)
        self._client_token = token
        self._owns_client = True
        return self._client

    def _get_client(self, *, force_refresh: bool = False) -> Any:
        self._ensure_open()
        token: str | None = None
        if self._client is not None:
            if self._owns_client and self.config.token_provider is not None and not self.in_transaction:
                token = self.config.resolve_iam_password_sync(force_refresh=force_refresh)
                if not force_refresh and token == self._client_token:
                    return self._client
                stale = self._client
                self._client = None
                self._client_token = None
                if self._has_broker:
                    release_fn: Callable[[], Any] | None = getattr(stale, "release_lease", None)
                    if callable(release_fn):
                        with contextlib.suppress(Exception):
                            release_fn()
                close_fn: Callable[[], Any] | None = getattr(stale, "close", None)
                if callable(close_fn):
                    with contextlib.suppress(Exception):
                        res = close_fn()
                        if inspect.isawaitable(res):
                            close_coro: Callable[[], Any] | None = getattr(res, "close", None)
                            if callable(close_coro):
                                close_coro()
            else:
                return self._client
        elif self.config.token_provider is not None:
            token = self.config.resolve_iam_password_sync(force_refresh=force_refresh)
        client = self._create_native_client(token)
        if self._has_broker:
            self._attach_checkout_lease(client)
        return client

    async def _close_owned_client_async(self) -> None:
        client = self._client
        if self._owns_client:
            self._client = None
            self._client_token = None
        if client is not None:
            if self._has_broker:
                release_fn: Callable[[], Any] | None = getattr(client, "release_lease", None)
                if callable(release_fn):
                    with contextlib.suppress(Exception):
                        release_fn()
            close_fn: Callable[[], Any] | None = getattr(client, "close", None)
            if callable(close_fn):
                with CancelScope(shield=True), contextlib.suppress(Exception):
                    res = close_fn()
                    if inspect.isawaitable(res):
                        await res

    async def _get_client_async(self, *, force_refresh: bool = False) -> Any:
        self._ensure_open()
        token: str | None = None
        if self._client is not None:
            if self._owns_client and self.config.token_provider is not None and not self.in_transaction:
                token = await self.config.resolve_iam_password_async(force_refresh=force_refresh)
                if not force_refresh and token == self._client_token:
                    return self._client
                await self._close_owned_client_async()
            else:
                return self._client
        elif self.config.token_provider is not None or self.config.enable_iam_auth:
            token = await self.config.resolve_iam_password_async(force_refresh=force_refresh)
        client = self._create_native_client(token, skip_iam_sync=True)
        if self._has_broker:
            await self._attach_checkout_lease_async(client)
        return client

    async def _refresh_client_after_auth_error(self) -> None:
        invalidate_fn: Callable[[], Any] | None = getattr(self.config.token_provider, "invalidate", None)
        if callable(invalidate_fn):
            invalidate_fn()
        if self._owns_client:
            client = self._client
            if self._has_broker and client is not None:
                release_fn: Callable[[], Any] | None = getattr(client, "release_lease", None)
                if callable(release_fn):
                    with contextlib.suppress(Exception):
                        release_fn()
            await self._get_client_async(force_refresh=True)
        else:
            await self.config.resolve_iam_password_async(force_refresh=True)

    async def _run_with_client[T](
        self,
        op: Callable[[Any], Awaitable[T]],
        *,
        map_errors: bool = True,
    ) -> T:
        for attempt in (0, 1):
            client = await self._get_client_async()
            try:
                return await op(client)
            except Exception as exc:
                if (
                    attempt == 0
                    and self.config.token_provider is not None
                    and not self.in_transaction
                    and is_mysql_auth_rejection(exc)
                ):
                    await self._refresh_client_after_auth_error()
                    continue
                if not map_errors or isinstance(exc, DatabaseError):
                    raise
                raise MySQLCore.map_exception(exc) from exc
        msg = "MySQL authentication retry exhausted"
        raise OperationalError(msg)
