"""Asynchronous native Oracle 26ai Thin driver and connection pool."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from operator import attrgetter
from typing import TYPE_CHECKING, Any, ClassVar, Self

from vertebra.db.adapters.oracle.config import OracleConfig, OracleSessionAttrs
from vertebra.db.adapters.oracle.core import ORACLE_DIALECT_TAG, OracleCore
from vertebra.db.adapters.oracle.notify import (
    AsyncOracleNotificationSource,
    OracleNotifyConfig,
    validate_oracle_channel_name,
)
from vertebra.db.adapters.oracle.pipeline import AsyncOraclePipeline
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import AsyncDatabaseDriver, InterfaceError, NotificationEvent
from vertebra.db.driver._common import _lease_releaser
from vertebra.db.rls import TenantScopeConfig
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._oracle import AsyncOracleCursor
    from vertebra.db.cdc import ChangeFeed

__all__ = (
    "AsyncOracleDriver",
    "AsyncOraclePool",
)

_DEFAULT_TENANT_SCOPE = TenantScopeConfig()
_ASYNC_PINNED_TX: ContextVar[dict[int, tuple[int, int | None, Any, bool]] | None] = ContextVar(
    "_ASYNC_PINNED_TX",
    default=None,
)
_FETCH_ROWS = attrgetter("fetch_rows")
_FETCH_DICTS = attrgetter("fetch_dicts")
_FETCH_VALUE = attrgetter("fetch_value")
_EXECUTE_DML = attrgetter("execute_dml")


class AsyncOraclePool:
    """Asynchronous physical Oracle connection pool backed by `vertebra._oracle.AsyncOraclePool`."""

    def __init__(
        self,
        config: OracleConfig | None = None,
        *,
        native_pool: Any | None = None,
        **kwargs: Any,
    ) -> None:
        resolved_cfg, pool_kwargs = OracleConfig.resolve_pool_kwargs(config, kwargs)
        self._config: OracleConfig = resolved_cfg
        if native_pool is not None:
            self._pool = native_pool
        else:
            mod = OracleCore.require_native_module()
            self._pool = mod.AsyncOraclePool(**pool_kwargs)

    @property
    def config(self) -> OracleConfig:
        """Return the `OracleConfig` governing this connection pool."""
        return self._config

    @property
    def native_pool(self) -> Any:
        """Return the underlying `vertebra._oracle.AsyncOraclePool` instance."""
        return self._pool

    @property
    def min_size(self) -> int:
        """Minimum number of pooled physical connections."""
        return int(self._pool.min_size)

    @property
    def max_size(self) -> int:
        """Maximum number of pooled physical connections."""
        return int(self._pool.max_size)

    @property
    def size(self) -> int:
        """Current total number of open + opening connections in the pool."""
        return int(self._pool.size)

    @property
    def idle_size(self) -> int:
        """Current number of idle connections available for checkout."""
        return int(self._pool.idle_size)

    @property
    def borrowed_size(self) -> int:
        """Current number of borrowed connections checked out by callers."""
        return int(self._pool.borrowed_size)

    @property
    def closed(self) -> bool:
        """Return `True` when the pool has been closed."""
        return bool(self._pool.closed)

    @property
    def is_closed(self) -> bool:
        """Return `True` when the pool has been closed."""
        return bool(self._pool.closed)

    async def acquire(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> Any:
        """Acquire an `AsyncOracleClient` lease from the pool asynchronously."""
        return await self._pool.acquire(
            timeout=timeout,
            connection_tag=connection_tag,
            purity=purity,
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )

    async def acquire_from_pool(self, **kwargs: Any) -> Any:
        """Acquire an `AsyncOracleClient` lease from the pool with DRCP tag and TTC piggyback options."""
        if hasattr(self._pool, "acquire_from_pool"):
            return await self._pool.acquire_from_pool(**kwargs)
        return await self._pool.acquire(**kwargs)

    async def release(
        self,
        client: Any,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None:
        """Release a borrowed `AsyncOracleClient` lease back to the pool asynchronously."""
        await self._pool.release(client, connection_tag=connection_tag, discard=discard)

    async def release_to_pool(self, client: Any, **kwargs: Any) -> None:
        """Release a borrowed `AsyncOracleClient` lease back to the pool with tag or discard flags."""
        if hasattr(self._pool, "release_to_pool"):
            await self._pool.release_to_pool(client, **kwargs)
        else:
            await self._pool.release(client, **kwargs)

    async def evict(self, client: Any) -> None:
        """Permanently evict and close a broken or poisoned `AsyncOracleClient`."""
        await self._pool.evict(client)

    def stats(self) -> dict[str, int]:
        """Return a snapshot dictionary of pool metrics."""
        return dict(self._pool.stats())

    @asynccontextmanager
    async def lease(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> AsyncIterator[AsyncOracleDriver]:
        """Check out a pooled connection wrapped in an `AsyncOracleDriver` and release on exit."""
        client = await self.acquire_from_pool(
            timeout=timeout,
            connection_tag=connection_tag,
            purity=purity,
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )
        drv = AsyncOracleDriver(self._config, client=client)
        if any((client_identifier, module, action, ecid)):
            drv._pending_session_attrs = None
        discard = False
        try:
            yield drv
        except BaseException as exc:
            discard = bool(getattr(client, "closed", False)) or OracleCore.is_fatal_transport_error(exc)
            raise
        finally:
            effective_tag = getattr(client, "connection_tag", None) or connection_tag
            await self.release_to_pool(
                client,
                connection_tag=effective_tag,
                discard=discard or bool(getattr(client, "closed", False)),
            )

    async def close(self) -> None:
        """Close the pool and all idle physical sessions asynchronously."""
        await self._pool.close()

    async def __aenter__(self) -> Self:
        """Enter the asynchronous pool context."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the asynchronous pool on context exit."""
        await self.close()


class AsyncOracleDriver(AsyncDatabaseDriver[OracleConfig]):
    """Asynchronous Oracle 26ai Thin driver backed by `vertebra._oracle.AsyncOracleClient`."""

    dialect: ClassVar[str] = "oracle"
    dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG

    def __init__(
        self,
        config: OracleConfig | None = None,
        *,
        broker: Any = None,
        client: Any = None,
        pool: AsyncOraclePool | Any | None = None,
    ) -> None:
        resolved = config or OracleConfig()
        super().__init__(resolved, broker=broker)
        self._pool: AsyncOraclePool | Any | None = pool
        if client is not None:
            self._client = client
        else:
            mod = OracleCore.require_native_module()
            self._client = mod.AsyncOracleClient(**resolved.to_client_kwargs())
        self._in_transaction: bool = False
        self._session_context: dict[tuple[str, str], str] = {}
        initial_attrs = self.config.session_attrs()
        self._pending_session_attrs: OracleSessionAttrs | None = (
            initial_attrs
            if any((initial_attrs.client_identifier, initial_attrs.module, initial_attrs.action, initial_attrs.ecid))
            else None
        )
        self._last_piggyback_frame: bytes | None = None
        self._pipe_savepoint_seq: int = 0
        self._pipeline_active: bool = False
        self._last_pipeline_outcomes: tuple[Any, ...] = ()
        self._last_out_binds: Any = None
        self._tx_events: list[str] = []
        features = self.config.driver_features
        self._cursor_opts: dict[str, Any] = {
            "enable_uuid_binary": features.enable_uuid_binary,
            "fetch_decimals": features.fetch_decimals,
            "vector_return_format": features.vector_return_format,
            "enable_lowercase_column_names": features.enable_lowercase_column_names,
        }
        self._lease_at_checkout: bool = self._broker is not None
        self._native_pool: Any = pool.native_pool if isinstance(pool, AsyncOraclePool) else pool
        self._pool_target: Any = None if self._lease_at_checkout else self._native_pool

    def _cursor_options(self) -> dict[str, Any]:
        return self._cursor_opts

    def cursor(self, *, lazy_begin: bool = False) -> AsyncOracleCursor:
        """Return a native `AsyncOracleCursor` on the pinned transaction client or single client."""
        active = self._get_pinned_lease()
        if active is None:
            if self._pool is not None:
                raise InterfaceError(
                    "cursor() needs an open transaction on a pooled Oracle driver; use checkout_cursor()"
                )
            active = self._client
        cur: AsyncOracleCursor = active.cursor(**self._cursor_options(), lazy_begin=lazy_begin)
        return cur

    @asynccontextmanager
    async def checkout_cursor(self, *, lazy_begin: bool = False) -> AsyncIterator[AsyncOracleCursor]:
        """Check out a client, yield a native `AsyncOracleCursor`, and close the cursor on exit."""
        async with self._checkout_client() as active_client:
            cur: AsyncOracleCursor = active_client.cursor(**self._cursor_options(), lazy_begin=lazy_begin)
            try:
                yield cur
            finally:
                await cur.close()

    @property
    def out_binds(self) -> Any:
        """Return the coerced `out_binds` payload from the most recent execution."""
        return self._last_out_binds

    @property
    def last_pipeline_outcomes(self) -> tuple[Any, ...]:
        """Return the `OraclePipelineOutcome` items from the most recent pipeline `results()` flush."""
        return self._last_pipeline_outcomes

    @property
    def transaction_events(self) -> tuple[str, ...]:
        """Return the ordered history of transaction lifecycle commands issued on this driver."""
        return tuple(self._tx_events)

    @property
    def pool(self) -> AsyncOraclePool | Any | None:
        """Return the optional `AsyncOraclePool` attached to this driver."""
        return self._pool

    @property
    def client(self) -> Any:
        """Return the pinned transaction `AsyncOracleClient` or the default underlying client."""
        pinned = self._get_pinned_lease()
        return pinned if pinned is not None else self._client

    @property
    def in_transaction(self) -> bool:
        """Return `True` when an explicit transaction is pinned in the current thread/task."""
        pinned = self._get_pinned_lease()
        if pinned is not None:
            return True
        if self._pool is not None:
            return False
        return bool(getattr(self._client, "in_transaction", self._in_transaction))

    @property
    def last_piggyback_frame(self) -> bytes | None:
        """Return the most recently piggybacked TTC `0x11` session attribute frame."""
        active = self._get_pinned_lease() or (self._client if self._pool is None else None)
        if active is not None:
            native_frame = getattr(active, "last_piggyback_frame", None)
            if native_frame is not None:
                return bytes(native_frame)
        return self._last_piggyback_frame

    def pipeline(self) -> AsyncOraclePipeline:
        """Create a delayed-commit asynchronous query pipeline bound to this driver."""
        if self._pipeline_active:
            raise InterfaceError("An Oracle pipeline is already active on this driver")
        return AsyncOraclePipeline(self)

    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None:
        """Queue updated session tracing attributes for the next SQL round-trip."""
        prev = self._pending_session_attrs
        merged = OracleSessionAttrs(
            client_identifier=client_identifier
            if client_identifier is not None
            else (prev.client_identifier if prev else None),
            module=module if module is not None else (prev.module if prev else None),
            action=action if action is not None else (prev.action if prev else None),
            ecid=ecid if ecid is not None else (prev.ecid if prev else None),
        )
        self._pending_session_attrs = merged
        active = self._get_pinned_lease() or (self._client if self._pool is None else None)
        if active is not None and hasattr(active, "set_session_attrs"):
            active.set_session_attrs(
                client_identifier=client_identifier,
                module=module,
                action=action,
                ecid=ecid,
            )

    async def connect(self) -> Self:
        """Establish or validate the underlying asynchronous Oracle session."""
        if self._pool is None:
            await self._client.connect()
            if self._lease_at_checkout:
                await self._attach_checkout_lease_async(self._client)
        self._closed = False
        return self

    async def ping(self) -> None:
        """Verify liveness of the underlying asynchronous Oracle session."""
        async with self._checkout_client() as active_client:
            await active_client.ping()

    async def begin(self) -> None:
        """Begin an explicit asynchronous Oracle transaction and pin the connection lease in a guarded `ContextVar`."""
        pinned = self._get_pinned_lease()
        if pinned is not None:
            await pinned.begin()
            self._in_transaction = True
            self._tx_events.append("BEGIN")
            return

        if self._pool is not None:
            lease = await self._acquire_pool_client()
            try:
                self._apply_pending_session_attrs(lease)
                await lease.begin()
            except BaseException as exc:
                await self._release_pool_client(
                    lease,
                    discard=bool(getattr(lease, "closed", False)) or OracleCore.is_fatal_transport_error(exc),
                )
                raise
            self._set_pinned_lease(lease, from_pool=True)
            self._in_transaction = True
            self._tx_events.append("BEGIN")
            return

        self._apply_pending_session_attrs(self._client)
        await self._client.begin()
        self._set_pinned_lease(self._client, from_pool=False)
        self._in_transaction = True
        self._tx_events.append("BEGIN")

    async def commit(self) -> None:
        """Commit the active asynchronous Oracle transaction and release any pinned pool lease."""
        entry = self._pop_pinned_entry()
        self._in_transaction = False
        self._tx_events.append("COMMIT")
        if entry is not None:
            _, _, active_client, from_pool = entry
            discard = False
            try:
                await active_client.commit()
            except BaseException as exc:
                discard = bool(getattr(active_client, "closed", False)) or OracleCore.is_fatal_transport_error(exc)
                raise
            finally:
                self._capture_piggyback(active_client)
                if from_pool and self._pool is not None:
                    await self._release_pool_client(active_client, discard=discard)
            return

        if self._pool is None:
            try:
                await self._client.commit()
            finally:
                self._capture_piggyback(self._client)

    async def rollback(self) -> None:
        """Roll back the active asynchronous Oracle transaction and release any pinned pool lease."""
        entry = self._pop_pinned_entry()
        self._in_transaction = False
        self._tx_events.append("ROLLBACK")
        if entry is not None:
            _, _, active_client, from_pool = entry
            discard = False
            try:
                await active_client.rollback()
            except BaseException:
                discard = True
                raise
            finally:
                self._capture_piggyback(active_client)
                if from_pool and self._pool is not None:
                    await self._release_pool_client(active_client, discard=discard)
            return

        if self._pool is None:
            try:
                await self._client.rollback()
            finally:
                self._capture_piggyback(self._client)

    async def savepoint(self, name: str = "sp") -> None:
        """Create a transaction savepoint asynchronously with identifier validation."""
        validate_identifier(name, "savepoint")
        active_client = self._get_pinned_lease() or self._client
        await active_client.savepoint(name)
        self._in_transaction = True
        self._tx_events.append(f"SAVEPOINT {name}")

    async def release_savepoint(self, name: str = "sp") -> None:
        """Validate savepoint identifier asynchronously (`RELEASE SAVEPOINT` is a validated no-op in Oracle SQL)."""
        validate_identifier(name, "savepoint")

    async def rollback_to_savepoint(self, name: str = "sp") -> None:
        """Roll back to a previously established savepoint asynchronously with identifier validation."""
        validate_identifier(name, "savepoint")
        active_client = self._get_pinned_lease() or self._client
        await active_client.rollback_to_savepoint(name)
        self._in_transaction = True
        self._tx_events.append(f"ROLLBACK TO SAVEPOINT {name}")

    async def cancel(self) -> None:
        """Trigger an out-of-band `break_execution()` on any in-flight command."""
        if not self._closed:
            active_client = self._get_pinned_lease() or self._client
            active_client.cancel()

    async def close(self) -> None:
        """Close the underlying native asynchronous Oracle client and release any pinned lease."""
        self._closed = True
        self._in_transaction = False
        entry = self._pop_pinned_entry()
        if entry is not None:
            _, _, active_client, from_pool = entry
            if from_pool and self._pool is not None:
                with suppress(Exception):
                    await self._release_pool_client(active_client, discard=False)
        if self._pool is None and self._lease_at_checkout:
            with suppress(Exception):
                self._client.release_lease()
        await self._client.close()

    async def enter_tenant_scope(
        self,
        tenant_value: str,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
    ) -> str:
        """Enter a session-level VPD tenant scope asynchronously using trusted package or `SET_IDENTIFIER`."""
        namespace = self.config.context_namespace
        sql, params = OracleCore.build_session_context_sql(
            str(tenant_value),
            config,
            namespace=namespace,
            context_package_proc=self.config.context_package_proc,
        )
        await self.execute(sql, params)
        attribute = config.setting_name.rsplit(".", 1)[-1]
        self._session_context[(namespace, attribute)] = str(tenant_value)
        return str(tenant_value)

    async def exit_tenant_scope(
        self,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
    ) -> None:
        """Reset session-level VPD tenant context asynchronously using trusted clear procedure or `CLEAR_IDENTIFIER`."""
        namespace = self.config.context_namespace
        sql, params = OracleCore.build_clear_context_sql(
            config,
            namespace=namespace,
            context_clear_proc=self.config.context_clear_proc,
        )
        try:
            if params:
                await self.execute(sql, params)
            else:
                await self.execute(sql)
        except BaseException:
            await self.evict_connection()
            raise
        attribute = config.setting_name.rsplit(".", 1)[-1]
        self._session_context.pop((namespace, attribute), None)

    async def evict_connection(self) -> None:
        """Permanently evict or close the active asynchronous physical session so tainted tenant state is never reused."""
        self._session_context.clear()
        self._in_transaction = False
        entry = self._pop_pinned_entry()
        if entry is not None:
            _, _, active_client, from_pool = entry
            if from_pool and self._pool is not None:
                with suppress(Exception):
                    await self._release_pool_client(active_client, discard=True)
                return
        self._closed = True
        if self._pool is None and self._lease_at_checkout:
            with suppress(Exception):
                self._client.release_lease()
        with suppress(Exception):
            await self._client.close()

    def _single_call(
        self,
        pick: Callable[[Any], Callable[..., Awaitable[Any]]],
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
        out_binds: Any,
    ) -> Awaitable[Any]:
        """Return the awaitable of one native single-call method for one statement."""
        if self._pending_session_attrs is None:
            target = self._get_pinned_lease()
            if target is None:
                target = self._client if self._pool is None else self._pool_target
            if target is not None:
                return pick(target)(sql, parameters or (), out_binds=out_binds, **self._cursor_opts)
        return self._single_call_checked(pick, sql, parameters, out_binds)

    async def _single_call_checked(
        self,
        pick: Callable[[Any], Callable[..., Awaitable[Any]]],
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
        out_binds: Any,
    ) -> Any:
        """Run one statement on a client after applying session attributes or checking it out of the pool."""
        active = self._get_pinned_lease()
        if active is None and self._pool is None:
            active = self._client
        if active is not None:
            self._apply_pending_session_attrs(active)
            try:
                return await pick(active)(sql, parameters or (), out_binds=out_binds, **self._cursor_opts)
            finally:
                self._capture_piggyback(active)
        lease = await self._acquire_pool_client()
        discard = False
        try:
            return await pick(lease)(sql, parameters or (), out_binds=out_binds, **self._cursor_opts)
        except BaseException as exc:
            discard = bool(getattr(lease, "closed", False)) or OracleCore.is_fatal_transport_error(exc)
            raise
        finally:
            await self._release_pool_client(lease, discard=discard)

    async def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[int, Any]:
        """Execute a statement and return its row count and last row id."""
        rowcount, lastrowid, out_values, touches = await self._single_call(
            _EXECUTE_DML, sql, parameters, kwargs.get("out_binds")
        )
        self._last_out_binds = out_values
        if touches:
            OracleCore.track_session_context_sql(self._session_context, sql, parameters)
        return rowcount, lastrowid

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML, DDL, or PL/SQL statement asynchronously and return affected row count."""
        rowcount, _ = await self.execute_dml(sql, parameters, **kwargs)
        return rowcount

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a DML statement across a batch of parameter sets asynchronously."""
        _ = kwargs
        if not parameter_sets:
            return 0
        async with self.checkout_cursor() as cur:
            await cur.executemany(sql, parameter_sets)
            return max(cur.rowcount, 0)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query or `DML ... RETURNING ... INTO` statement and return positional tuples."""
        self._last_out_binds = None
        rows, _ = await self._single_call(_FETCH_ROWS, sql, parameters, kwargs.get("out_binds"))
        result: list[tuple[Any, ...]] = rows
        return result

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL asynchronously and return `(rows, column_names)`."""
        self._last_out_binds = None
        result: tuple[list[tuple[Any, ...]], tuple[str, ...]] = await self._single_call(
            _FETCH_ROWS, sql, parameters, kwargs.get("out_binds")
        )
        return result

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` or `DML ... RETURNING` query asynchronously and hydrate rows."""
        self._last_out_binds = None
        out_binds = kwargs.get("out_binds")
        if schema_type is None:
            dicts: list[Any] = await self._single_call(_FETCH_DICTS, sql, parameters, out_binds)
            return dicts
        rows, names = await self._single_call(_FETCH_ROWS, sql, parameters, out_binds)
        return _hydrate_rows_with_schema(rows, names, schema_type) if rows else []

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        self._last_out_binds = None
        value = await self._single_call(_FETCH_VALUE, sql, parameters, kwargs.get("out_binds"))
        if value_type is not None and value is not None:
            return to_value_type(value, value_type)
        return value

    async def exec_returning_async(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `DML ... RETURNING ... INTO` statement in one round-trip and return hydrated rows."""
        return await self.select(sql, parameters, schema_type=schema_type, **kwargs)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int | None = None,
        force_shm: bool | None = None,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return a zero-copy `ArrowCursor`."""
        if hasattr(self._client, "query_arrow") and (
            force_shm is not None or threshold_rows == ARROW_SHM_THRESHOLD_ROWS
        ):
            coerced = OracleCore.coerce_parameters(parameters, features=self.config.driver_features)
            async with self._checkout_client() as active_client:
                OracleCore.track_session_context_sql(self._session_context, sql, parameters)
                effective_batch = int(batch_size) if batch_size is not None and batch_size > 0 else 8192
                cursor: ArrowCursor = await active_client.query_arrow(
                    sql,
                    coerced,
                    batch_size=effective_batch,
                    force_shm=force_shm,
                    enable_lowercase_column_names=self.config.driver_features.enable_lowercase_column_names,
                )
                return cursor
        rows, raw_cols = await self.execute_with_description(sql, parameters, **kwargs)
        cols = [col.lower() if self.config.driver_features.enable_lowercase_column_names else col for col in raw_cols]
        return ArrowCursor.from_rows(rows, cols, threshold_rows=threshold_rows)

    async def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        **kwargs: Any,
    ) -> AsyncIterator[ArrowCursor]:
        """Asynchronously stream query results as a sequence of `ArrowCursor` batches."""
        chunk = max(1, int(batch_size))
        rows, cols = await self.execute_with_description(sql, parameters, **kwargs)
        for idx in range(0, len(rows), chunk):
            yield ArrowCursor.from_rows(rows[idx : idx + chunk], cols)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-ingest an Arrow Table or `ArrowCursor` into `table_name`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column_name", allow_dots=False)
        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column_name", allow_dots=False)
        if not rows or not cols:
            return 0
        use_direct_path = (
            self.config.driver_features.enable_direct_path_load if direct_path is None else bool(direct_path)
        )
        hint = "/*+ APPEND_VALUES */ " if use_direct_path else ""
        placeholders = ", ".join(f":{i + 1}" for i in range(len(cols)))
        col_list = ", ".join(str(col) for col in cols)
        sql = f"INSERT {hint}INTO {table_name} ({col_list}) VALUES ({placeholders})"
        return await self.execute_many(sql, rows)

    def notification_source(
        self,
        config: OracleNotifyConfig | None = None,
        *,
        change_feed: ChangeFeed | None = None,
    ) -> AsyncOracleNotificationSource:
        """Construct an `AsyncOracleNotificationSource` bound to this asynchronous driver."""
        return AsyncOracleNotificationSource(self, config, change_feed=change_feed)

    async def publish_notification(
        self,
        channel: str,
        payload: bytes | str,
        *,
        queue_prefix: str = "VTB_AQ_",
        consumer_name: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Enqueue an AQ / TxEventQ notification asynchronously and return its message ID."""
        queue_name = validate_oracle_channel_name(channel, queue_prefix=queue_prefix)
        raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        async with self._checkout_client() as active_client:
            msg_id_bytes: bytes = await active_client.enqueue_notification(
                queue_name=queue_name,
                payload=raw_bytes,
                consumer_name=consumer_name,
                correlation_id=correlation_id,
            )
            return bytes(msg_id_bytes).decode("ascii", errors="replace")

    async def listen_notifications(
        self,
        channels: Sequence[str],
        *,
        config: OracleNotifyConfig | None = None,
    ) -> AsyncIterator[NotificationEvent]:
        """Subscribe to `channels` and yield `NotificationEvent` frames with automatic cleanup."""
        source = AsyncOracleNotificationSource(self, config)
        await source.subscribe(channels)
        try:
            async for event in source.listen():
                yield event
        finally:
            await source.close()

    def _get_pinned_lease(self) -> Any | None:
        mapping = _ASYNC_PINNED_TX.get()
        if not mapping:
            return None
        entry = mapping.get(id(self))
        if entry is None:
            return None
        stored_thread, stored_task, lease, _ = entry
        curr_thread, curr_task = OracleCore.current_execution_identity()
        if stored_thread != curr_thread or stored_task != curr_task:
            return None
        return lease

    def _set_pinned_lease(self, lease: Any, *, from_pool: bool) -> None:
        curr_thread, curr_task = OracleCore.current_execution_identity()
        existing = _ASYNC_PINNED_TX.get()
        updated = dict(existing) if existing is not None else {}
        updated[id(self)] = (curr_thread, curr_task, lease, from_pool)
        _ASYNC_PINNED_TX.set(updated)

    def _pop_pinned_entry(self) -> tuple[int, int | None, Any, bool] | None:
        mapping = _ASYNC_PINNED_TX.get()
        if not mapping:
            return None
        entry = mapping.get(id(self))
        if entry is None:
            return None
        stored_thread, stored_task, _, _ = entry
        curr_thread, curr_task = OracleCore.current_execution_identity()
        if stored_thread != curr_thread or stored_task != curr_task:
            return None
        updated = dict(mapping)
        updated.pop(id(self), None)
        _ASYNC_PINNED_TX.set(updated or None)
        return entry

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
            with suppress(RuntimeError):
                return lease_table.acquire(os.getpid())
        return None

    async def _attach_checkout_lease_async(self, client: Any, timeout_ms: int = 5000) -> bool:
        lease = await self._acquire_broker_lease_async(timeout_ms)
        if lease is None:
            return False
        client.attach_lease(_lease_releaser(self._broker, self._lease_plan(), lease))
        return True

    async def _acquire_pool_client(self) -> Any:
        if self._pool is None:
            raise InterfaceError("Oracle connection pool is not configured")
        attrs = self._pending_session_attrs
        self._pending_session_attrs = None
        acquire_kwargs = OracleCore.build_session_attr_acquire_kwargs(attrs)
        if hasattr(self._pool, "acquire_from_pool"):
            client = await self._pool.acquire_from_pool(**acquire_kwargs)
        else:
            client = await self._pool.acquire(**acquire_kwargs)
        if self._lease_at_checkout:
            try:
                await self._attach_checkout_lease_async(client)
            except BaseException:
                await self._release_pool_client(client, discard=False)
                raise
        return client

    async def _release_pool_client(self, lease: Any, *, discard: bool = False) -> None:
        if self._pool is None:
            return
        self._capture_piggyback(lease)
        if self._lease_at_checkout:
            with suppress(Exception):
                lease.release_lease()
        if hasattr(self._pool, "release_to_pool"):
            await self._pool.release_to_pool(lease, discard=discard)
        else:
            await self._pool.release(lease, discard=discard)

    @asynccontextmanager
    async def _checkout_client(self) -> AsyncIterator[Any]:
        pinned = self._get_pinned_lease()
        if pinned is not None:
            self._apply_pending_session_attrs(pinned)
            try:
                yield pinned
            finally:
                self._capture_piggyback(pinned)
            return

        if self._pool is None:
            self._apply_pending_session_attrs(self._client)
            try:
                yield self._client
            finally:
                self._capture_piggyback(self._client)
            return

        lease = await self._acquire_pool_client()
        discard = False
        try:
            yield lease
        except BaseException as exc:
            discard = bool(getattr(lease, "closed", False)) or OracleCore.is_fatal_transport_error(exc)
            raise
        finally:
            self._capture_piggyback(lease)
            await self._release_pool_client(lease, discard=discard)

    def _apply_pending_session_attrs(self, client: Any) -> None:
        if self._pending_session_attrs is None:
            return
        attrs = self._pending_session_attrs
        self._pending_session_attrs = None
        if any((attrs.client_identifier, attrs.module, attrs.action, attrs.ecid)):
            if hasattr(client, "set_session_attrs"):
                client.set_session_attrs(
                    client_identifier=attrs.client_identifier,
                    module=attrs.module,
                    action=attrs.action,
                    ecid=attrs.ecid,
                )
            else:
                self._last_piggyback_frame = attrs.encode_ttc_piggyback()

    def _capture_piggyback(self, client: Any) -> None:
        frame = getattr(client, "last_piggyback_frame", None)
        if frame is not None:
            self._last_piggyback_frame = bytes(frame)
